package com.lucidworks.dq.data;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;

import com.lucidworks.dq.util.DateUtils;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;
import com.lucidworks.dq.util.StringUtils;

public class SolrToCsv {
  static String HELP_WHAT_IS_IT = "Export records from Solr collection or core to delimited file, such as CSV.";
  static String EXTENDED_DESCRIPTION =
    StringUtils.NL
    + "Useful for quickly exporting data from standalone Solr or SolrCloud."
    + " WARNING: Early prototype: Use the query parameter to insure that all your fields have values"
    + "; this version doesn't output placeholder fields for missing values!"
    + " Can ONLY export STORED Fields, though this is the default for many fields in Solr."
    + " Will use Solr \"Cursor Marks\", AKA \"Deep Paging\", if available"
    + " which is in Solr version 4.7+"
    + ", see https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results and SOLR-5463"
    + StringUtils.NL
    + "Options:"
    ;
  static String HELP_USAGE = "SolrToSolr --url_a http://localhost:8983/collection1 --url_b http://localhost:8983/collection2";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  // See batch size comments at bottom of file
  static int DEFAULT_BATCH_SIZE = 1000;
  
  static String DEFAULT_QUERY = "*:*";

  // TODO: read this from server
  static String ID_FIELD = "id";
  static String VERSION_FIELD = "_version_";
  static String ROOT_FIELD = "_root_";
  static List<String> DEFAULT_EXCLUDE_FIELDS = Arrays.asList( new String[]{
      VERSION_FIELD,
      ROOT_FIELD
  });

  static int COMMIT_DELAY = 30000; // 30k ms: Commit within 30 seconds

  static Options options;

  HttpSolrServer solr;
  int batchSize = DEFAULT_BATCH_SIZE;
  String query = DEFAULT_QUERY;
  boolean ignoreCase = false;
  boolean _outputLowercase = false;

  // leave these null by default
  Set<String> includeFields;
  Set<String> includeLiterals;
  Set<Pattern> includePatterns;
  Set<String> excludeFields;
  Set<String> excludeLiterals;
  Set<Pattern> excludePatterns;

  boolean supportsCursorMarks;
  private String encodingStr;
  private boolean useStrictEncoding;
  private boolean keepTroublesomeRecords;
  private String outputFile;
  private PrintWriter out;
  private String delimiter;

  public SolrToCsv( HttpSolrServer source ) throws SolrServerException {
    this.solr = source;
    this.supportsCursorMarks = SolrUtils.checkCursorMarkPagingSupport(this.solr);
    System.out.println( "Solr source supportsCursorMarks = " + this.supportsCursorMarks );
  }

  public long processAll() throws SolrServerException, IOException {
    setupOutputFile();

    SolrQuery q = prepInitialQuery();

    long submittedCount = 0L;
    // Finish up depending on whether we support modern cursors or not
    if (isSupportsCursorMarks() && getBatchSize() > 0) {
      submittedCount = processAllWithCursors(q);
    } else {
      submittedCount = processAllWithOldPaging(q);
    }
    
    closeOutputFile();

    return submittedCount;
  }
  SolrQuery prepInitialQuery() throws SolrServerException {
    // TODO: could put query as fq/filter query to bypass Relevancy sorting
    SolrQuery q = new SolrQuery(getQuery());
    System.out.println( "Fetching rows that match query " + getQuery() );
    // If any restriction on fields, then faster to only fetch fields that we need
    if (null != getIncludeFields() || null != getExcludeFields()) {
      Set<String> storedFields = SolrUtils.getLukeFieldsWithStoredValues(solr);
      storedFields = filterFields(storedFields);
      System.out.println( "Requesting fields " + storedFields );
      for (String fieldName : storedFields) {
        q.addField(fieldName);
      }
    } else {
      System.out.println( "Requesting all fields" );
      q.addField("*");
    }
    if (getBatchSize() > 0) {
      System.out.println( "Batch size: " + getBatchSize() );
      q.setRows(getBatchSize());
    } else {
      // one big gulp, usually a bad idea
      System.out.println( "Batch size: all-at-once" );
      q.setRows(Integer.MAX_VALUE);
    }
    return q;
  }
  public long processAllWithCursors( SolrQuery q ) throws SolrServerException, IOException {
    q.setSort( ID_FIELD, SolrQuery.ORDER.asc );
    String cursorMark = CursorMarkParams.CURSOR_MARK_START;
    boolean done = false;
    long docCount = 0L;
    while (! done) {
      // System.out.println( "About to query: cursorMark = \"" + cursorMark + "\", docCount = " + docCount );
      q.set( CursorMarkParams.CURSOR_MARK_PARAM, cursorMark );
      QueryResponse rsp = solr.query( q );
      String nextCursorMark = rsp.getNextCursorMark();

      Collection<SolrDocument> batch = new LinkedList<SolrDocument>();
      for (SolrDocument doc : rsp.getResults()) {
        docCount++;
        batch.add(doc);
      }
      processBatch(batch);
      logIntermediateProgressIfApplicable( batch.size(), docCount );

      if (cursorMark.equals(nextCursorMark)) {
        done = true;
      }
      cursorMark = nextCursorMark;
    }
    //System.out.println( "Done: cursorMark = \"" + cursorMark + "\", docCount = " + docCount );
    return docCount;
  }

  public long processAllWithOldPaging(SolrQuery q)
      throws SolrServerException, IOException
  {
    int start = 0;
    boolean done = false;
    long docCount = 0L;
    while (!done) {
      // System.out.println( "Starting at " + start + ", batchSize=" + getBatchSize() );
      q.setStart(start);
      QueryResponse rsp = solr.query(q);
      Collection<SolrDocument> batch = new LinkedList<SolrDocument>();
      SolrDocumentList docs = rsp.getResults();
      for (SolrDocument doc : docs) {
        docCount++;
        batch.add(doc);
      }
      processBatch(batch);
      logIntermediateProgressIfApplicable( batch.size(), docCount );
      long numFound = docs.getNumFound();
      // done = getBatchSize()<1 || start + getBatchSize() >= numFound;
      done = getBatchSize() < 1 || docCount >= numFound;
      if (getBatchSize() > 0) {
        start += getBatchSize();
      }
    }
    return docCount;
  }

  public void processBatch( Collection<SolrDocument> sourceDocs ) throws SolrServerException, IOException {
    if ( null==sourceDocs || sourceDocs.isEmpty() ) {
      return;
    }
    //Collection<SolrInputDocument> destinationDocs = new ArrayList<SolrInputDocument>();
    for ( SolrDocument srcDoc : sourceDocs ) {
      outputSolrResultsDocToCsv( srcDoc );
      //SolrInputDocument dstDoc = convertSolrResultsDocToSolrIndexingDoc( srcDoc );      
      //destinationDocs.add( dstDoc );
    }
    //solrB.add( destinationDocs, COMMIT_DELAY );
    //System.out.println( "Submitted " + destinationDocs.size() + " docs." );
  }

  void logIntermediateProgressIfApplicable( long currentBatch, long totalCount ) {
    // TODO: add command line flag and object property to suppress this
    // though very handy by default, to see if *anything* is happening

    String currTimeStr = DateUtils.getLocalTimestamp();
    String currentBatchStr = NumberFormat.getNumberInstance().format( currentBatch );
    String totalCountStr = NumberFormat.getNumberInstance().format( totalCount );

    System.out.print( currTimeStr );
    if ( currentBatch > 0L ) {
      System.out.print( " Exported " + currentBatchStr + " docs" );
    }
    if ( totalCount > 0L ) {
      System.out.print( " (total = " + totalCountStr + ")" );
    }
    if ( currentBatch <= 0L && totalCount <=0L ) {
      System.out.print( " WARNING: Not passed any stats to log!" );
    }
    System.out.println();
  }
  SolrInputDocument _convertSolrResultsDocToSolrIndexingDoc( SolrDocument sourceDoc ) {
    SolrInputDocument destinationDoc = new SolrInputDocument();
    for( Entry<String, Object> srcEntry : sourceDoc.entrySet() ) {
      String fieldName = srcEntry.getKey();
      // Skip internal fields like _version_
      // We still need to check fields here
      // Although we call filterFields when preparing the query
      // sometimes we just use fl=* so need to check here
      if ( ! checkFieldName(fieldName) ) {
        continue;
      }
      Object fieldValue = srcEntry.getValue();
//      if ( isOutputLowercase() ) {
//        fieldName = fieldName.toLowerCase();
//      }
      destinationDoc.addField( fieldName, fieldValue );
    }
    return destinationDoc;
  }
  // TODO: this doesn't maintain record position when intermediate fields are missing
  // So put id first, which will always have a value
  // Put fields that may be missing values at the end
  // If you have more than one of them, you'll have problems
  void outputSolrResultsDocToCsv( SolrDocument sourceDoc ) {
    // SolrInputDocument _destinationDoc = new SolrInputDocument();
    StringBuffer recordBuff = new StringBuffer();
    boolean sawBadField = false;
    // Foreach Field
    for( Entry<String, Object> srcEntry : sourceDoc.entrySet() ) {
      String fieldName = srcEntry.getKey();
      // Skip internal fields like _version_
      // We still need to check fields here
      // Although we call filterFields when preparing the query
      // sometimes we just use fl=* so need to check here
      if ( ! checkFieldName(fieldName) ) {
        continue;
      }
      Object fieldValue = srcEntry.getValue();
      String fieldValueStr = null;
      // System.out.println( "field name =" + fieldName );
      if ( fieldValue instanceof Collection ) {
        StringBuffer fieldBuff = new StringBuffer();
        Collection<?> values = (Collection) fieldValue;
        for ( Object v : values ) {
          if ( fieldBuff.length() > 0 ) {
            fieldBuff.append( " " );
          }
          fieldBuff.append( v.toString() );
        }
        fieldValueStr = new String( fieldBuff );
      }
      else {
        fieldValueStr = fieldValue.toString();
      }
      if ( fieldValueStr.indexOf(getDelimiter())>=0 ) {
        sawBadField = true;
      }
      if ( recordBuff.length()>0 ) {
        recordBuff.append( getDelimiter() );
      }
      recordBuff.append( fieldValueStr );
      // destinationDoc.addField( fieldName, fieldValue );
    }  // End Foreach Field
    if ( getKeepTroublesomeRecords() || ! sawBadField ) {
      out.write( new String(recordBuff) + "\n" );
    }

    //return destinationDoc;
  }

  void setupOutputFile() throws FileNotFoundException {
    // Setup IO encoding
    Charset charset = Charset.forName( encodingStr );
    // Input uses Decoder
    //CharsetDecoder decoder = charset.newDecoder();
    // Output uses Encoder
    CharsetEncoder encoder = charset.newEncoder();
    if ( useStrictEncoding ) {
      //decoder.onMalformedInput( CodingErrorAction.REPORT );
      encoder.onMalformedInput( CodingErrorAction.REPORT );
    }

    // PrintWriter out = null;
    if( null!=getOutputFile() && ! getOutputFile().equals("-") ) {
      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(getOutputFile()), encoder), true);
      System.out.println( "Exporting records to file " + getOutputFile() );
    } else {
      out = new PrintWriter(new OutputStreamWriter(System.out, encoder), true);
      System.out.println( "Exporting records to Standard Out" );
    }
  }
  void closeOutputFile() {
    out.close();
  }
  
  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getQuery() {
    return query;
  }
  public void setQuery(String query) {
    this.query = query;
  }

  public Set<String> getIncludeFields() {
    return includeFields;
  }
  // Need to set ignore_case before calling this
  public void setIncludeFields(Set<String> targetFields) {
    this.includeFields = targetFields;
    this.includeLiterals = new LinkedHashSet<>();
    this.includePatterns = new LinkedHashSet<>();
    analyzePatterns( this.includeFields, this.includeLiterals, this.includePatterns );
  }

  public Set<String> getExcludeFields() {
    return excludeFields;
  }
  // Need to set ignore_case before calling this
  public void setExcludeFields(Set<String> targetFields) {
    this.excludeFields = targetFields;
    this.excludeLiterals = new LinkedHashSet<>();
    this.excludePatterns = new LinkedHashSet<>();
    analyzePatterns( this.excludeFields, this.excludeLiterals, this.excludePatterns );
  }

  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }
  public String getOutputFile() {
    return this.outputFile;
  }

  public void setUseStrictEncoding(boolean strictEncoding) {
    this.useStrictEncoding = strictEncoding;
  }
  public boolean getUseStrictEncoding() {
    return this.useStrictEncoding;
  }

  public void setEncoding(String encodingStr) {
    this.encodingStr = encodingStr;
  }
  public String getEncoding() {
    return this.encodingStr;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }
  public String getDelimiter() {
    return this.delimiter;
  }

  public void setKeepTroublesomeRecords(boolean flag) {
    this.keepTroublesomeRecords = flag;
  }
  public boolean getKeepTroublesomeRecords() {
    return this.keepTroublesomeRecords;
  }

  private void analyzePatterns( Set<String> inCandidates, Set<String> outLiternals, Set<Pattern> outPatterns ) {
    for ( String origPattern : inCandidates ) {
      String newPattern = StringUtils.convertGlobToRegex( origPattern );
      // If unchanged then this is NOT a pattern, it's just a regular field name
      if ( newPattern.equals(origPattern) ) {
        if ( isIgnoreCase() ) {
          newPattern = newPattern.toLowerCase();
        }
        outLiternals.add( newPattern );
      }
      // Else it's a Regex
      else {
        int flags = Pattern.UNICODE_CASE;
        if ( isIgnoreCase() ) {
          flags |= Pattern.CASE_INSENSITIVE;
        }
        Pattern p = Pattern.compile( newPattern, flags );
        outPatterns.add( p );
      }
    }
  }

  // Run set of fields against include/exclude/system rules
  Set<String> filterFields( Set<String> inFields ) {
    Set<String> outFields = new LinkedHashSet<>();
    for ( String f : inFields ) {
      if ( checkFieldName(f) ) {
        outFields.add( f );
      }
    }
    return outFields;
  }

  // TODO: could use a unit test though pretty sure it's right
  // See main command line args' text for explanation
  boolean checkFieldName( String fieldName ) {
    if ( isIgnoreCase() ) {
      fieldName = fieldName.toLowerCase();
    }
    // Check Literals before Patterns
    // And Excludes before Includes
    if ( null!=excludeLiterals && excludeLiterals.contains(fieldName) ) {
      return false;
    }
    if ( null!=includeLiterals && includeLiterals.contains(fieldName) ) {
      return true;
    }
    // Excluded system fields like _version_ aren't eligible for pattern matching
    if ( DEFAULT_EXCLUDE_FIELDS.contains(fieldName) ) {
      return false;
    }
    // Now we check patterns
    if ( null!=excludePatterns && StringUtils.checkPatternsInList(excludePatterns, fieldName) ) {
      return false;
    }
    // Positive / Include logic is a bit different
    // Are we even using includes?
    if ( null!=includePatterns ) {
      // If using includes, then we require it to match
      // If it were a literal field we would have already returned true above
      //if ( StringUtils.checkPatternsInList(excludePatterns, fieldName) )
      if ( StringUtils.checkPatternsInList(includePatterns, fieldName) )
      {
        return true;
      }
      // Didn't match pattern
      // And didn't match earlier literal check either
      // and we ARE using Positive Logic so no match
      else {
        return false;
      }
    }
    // no include patterns to worry about
    else {
      // If we had a list of include literals and made it this far
      // then this field wasn't found there either
      // but it would mean that we ARE using Positive Logic
      // so it would be required to pass 1 of those 2 tests
      if ( null!=includeLiterals ) {
        return false;
      }
      // No include patterns nor literals, so allow everything
      else {
        return true;
      }
    }
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }
  public void setIgnoreCase(boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
  }

  public boolean _isOutputLowercase() {
    return _outputLowercase;
  }
  public void _setOutputLowercase(boolean outputLowercase) {
    this._outputLowercase = outputLowercase;
  }

  public boolean isSupportsCursorMarks() {
    return supportsCursorMarks;
  }
  public void setSupportsCursorMarks(boolean supportsCursorMarks) {
    this.supportsCursorMarks = supportsCursorMarks;
  }

  static void helpAndExit() {
    helpAndExit(null, 1);
  }
  static void helpAndExit(String optionalError, int errorCode) {
    HelpFormatter formatter = new HelpFormatter();
    if (null == optionalError) {
      System.err.println(HELP_WHAT_IS_IT);
    } else {
      // log.error( optionalError );
      System.err.println(optionalError);
    }
    // stdout
    // formatter.printHelp( HELP_USAGE, options, true );
    // stderr
    PrintWriter pw = new PrintWriter(System.err);
    // formatter.printHelp(pw, 78, HELP_USAGE, null, options, 1, 1, null, true);
    formatter.printHelp(pw, 78, HELP_USAGE, EXTENDED_DESCRIPTION, options, 1, 1, null, true);
    pw.flush();
    System.exit(errorCode);
  }

  public static void main(String[] argv) throws ParseException,
      SolrServerException, IOException
  {
    options = new Options();
    options.addOption( "u", "url", true, "URL for Solr, the source of the records");
    options.addOption( "h", "host", true, "IP address for Solr source");
    options.addOption( "p", "port", true, "Port for Solr source, default=8983");
    options.addOption( "c", "collection", true, "Collection/Core for Solr source, Eg: collection1");

    options.addOption( "q", "query", true, "Query to select which records will be copied; by default all records are copied.");

    options.addOption( "f", "include_fields", true,
            "Fields to copy, Eg: include_fields=id,name,category"
                + " Make sure to include the id!"
                + " By default all stored fields are included except system fields like _version_ and _root_."
                + " You can also use simple globbing patterns like billing_* but"
                + " make sure to use quotes on the command line to protect them from the operating system."
                + " Field name and pattern matching IS case sensitive unless you set ignore_case."
                + " Patterns do NOT match system fields either, so if really need a field like _version_"
                + " then add the full name to include_fields not using a wildcard."
                + " Solr field names should not contain commas, spaces or wildcard pattern characters."
                + " Does not use quite the same rules as dynamicField pattern matching, different implementation."
                + " See also exclude_fields");
    options.addOption( "F", "exclude_fields", true,
            "Fields to NOT copy over, Eg: exclude_fields=timestamp,text_en"
                + " Useful for skipping fields that will be re-populated by copyField in SolrB schema.xml."
                + " System fields like _version_ are already skipped by default."
                + " Use literal field names or simple globbing patterns like text_*"
                + "; remember to use quotes on the command line to protect wildcard characters from the operating system."
                + " Excludes override includes when comparing literal field names or when comparing patterns"
                + ", except that literal fields always take precedence over patterns."
                + " If a literal field name appears in both include and exclude, it will not be included."
                + " If a field matches both include and exclude patterns, it will not be included."
                + " However, if a field appears as a literal include but also happens to match an exclude pattern"
                + ", then the literal reference will win and it WILL be included."
                + " See also include_fields");

    options.addOption( "i", "ignore_case", false,
            "Ignore UPPER and lowercase differences when matching field names and patterns, AKA case insensitive"
                + "; the original form of the fieldname will still be output to the destination collection"
                + " unless output_lowercase_names is used");

    options.addOption( "d", "delimiter", true, "Field separator for output records, default is a comma.");

    options.addOption( "o", "output_file", true, "Output file to export data to (default or \"-\" is stdout / standard out)" );
    options.addOption( "e", "encoding", true, "Character Encoding for writing files (default is UTF-8, which enables cross-platform operation)" );
    options.addOption( "l", "loose_encoding", false, "Disable strict character encoding so that problems don't throw Exceptions (NOT recommended)" );

    
    options.addOption( "k", "keep_troublesome_records", false,
            "Normally we skip the entire record if any field contains your delimiter character."
            + " Why? This quickie version doesn't have proper CSV encoding support."
            + " Therefore, if a field value contains your delimiter character, it would NOT be handled correctly."
            + " Mutli-value fields are concatenated with spaces."
            + " Newlines are replaced with spaces.");

    options.addOption( "x", "xml", false,
            "Use XML transport (XMLResponseParser) instead of default javabin; useful when working with older versions of Solr, though slightly slower."
                + " Helps fix errors \"RuntimeException: Invalid version or the data in not in 'javabin' format\""
                + ", \"org.apache.solr.common.util.JavaBinCodec.unmarshal\", or similar errors.");

    // TODO: Multivalue options?
    // TODO: start at a certain offset or limit total number of records
    // TODO: sort options?
    // TODO: commit options

    options.addOption(
        OptionBuilder.withLongOpt("batch_size")
          .withDescription(
            "Batch size from Solr, 1=doc-by-doc."
            + " 0=all-at-once but be careful memory-wise and 0 also disables deep paging cursors."
            + " Default=" + DEFAULT_BATCH_SIZE
            + " Does NOT affect size of output data file, which just puts everything into one giant file (in this early version)."
            ).hasArg().withType(Number.class) // NOT Long.class
          .create("b"));

    if (argv.length < 1) {
      helpAndExit();
    }
    CommandLine cmd = null;
    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException exp) {
      helpAndExit("Parsing command line failed. Reason: " + exp.getMessage(), 2);
    }

    String fullUrl = cmd.getOptionValue("url");
    String host = cmd.getOptionValue("host");
    String port = cmd.getOptionValue("port");
    String coll = cmd.getOptionValue("collection");
    if (null == fullUrl && null == host) {
      helpAndExit( "Must specifify at least url or host for Solr source", 3);
    }
    if (null != fullUrl && null != host) {
      helpAndExit( "Must not specifify both url and host for Solr source", 4);
    }

    // Init
    HttpSolrServer solr;
    if (null != fullUrl) {
      solr = SolrUtils.getServer(fullUrl);
    } else {
      // Utils handle null values
      solr = SolrUtils.getServer(host, port, coll);
    }
    System.out.println("Solr Source = " + solr.getBaseURL());

    // Setup older Legacy transport mechanism, if requested
    if (cmd.hasOption("xml")) {
      solr.setParser(new XMLResponseParser());
    }

    // Main instance
    SolrToCsv solr2csv = new SolrToCsv(solr);

    // Additional options

    Long batchObj = (Long) cmd.getParsedOptionValue("batch_size");
    if (null != batchObj) {
      if (batchObj.longValue() < 0L) {
        helpAndExit("batch_size must be >= 0", 5);
      }
      solr2csv.setBatchSize(batchObj.intValue());
    }

    // ignore_case
    // Important! Set this BEFORE calling setIncludeFields or setExcludeFields
    // TODO: revisit this, some way to enforce it or do a final check
    if (cmd.hasOption("ignore_case")) {
      solr2csv.setIgnoreCase(true);
    }

    String includeFieldsStr = cmd.getOptionValue("include_fields");
    if (null != includeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(includeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        solr2csv.setIncludeFields(fields);
      }
    }

    String excludeFieldsStr = cmd.getOptionValue("exclude_fields");
    if (null != excludeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(excludeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        solr2csv.setExcludeFields(fields);
      }
    }

    if (cmd.hasOption("keep_troublesome_records")) {
      solr2csv.setKeepTroublesomeRecords(true);
    }

    String delim = cmd.getOptionValue("delimiter");
    if (null != delim) {
      solr2csv.setDelimiter(delim);
    }
    else {
      solr2csv.setDelimiter(",");
    }

    String queryStr = cmd.getOptionValue("query");
    if (null != queryStr) {
      solr2csv.setQuery(queryStr);
    }

    // File IO
    String outputFile = cmd.getOptionValue( "output_file" );
    if ( null!=outputFile || outputFile.trim().length()>0 ) {
      solr2csv.setOutputFile(outputFile);
    }
    String encodingStr = cmd.getOptionValue( "encoding" );
    if ( null==encodingStr || encodingStr.trim().length()<1 ) {
      encodingStr = "UTF-8";
    }
    solr2csv.setEncoding(encodingStr);
    boolean strictEncoding = true;
    if(cmd.hasOption("loose_encoding")) {
      strictEncoding = false;
    }
    solr2csv.setUseStrictEncoding(strictEncoding);

    // now handled in setupOutput(), called at top of processAll()
    /***
    // Setup IO encoding
    Charset charset = Charset.forName( encodingStr );
    // Input uses Decoder
    //CharsetDecoder decoder = charset.newDecoder();
    // Output uses Encoder
    CharsetEncoder encoder = charset.newEncoder();
    if ( strictEncoding ) {
      //decoder.onMalformedInput( CodingErrorAction.REPORT );
      encoder.onMalformedInput( CodingErrorAction.REPORT );
    }

    PrintWriter out = null;
    if( null!=outputFile && ! outputFile.equals("-") ) {
      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), encoder), true);
    } else {
      out = new PrintWriter(new OutputStreamWriter(System.out, encoder), true);
    }
    ***/
    
    
    // Do it!
    long start = System.currentTimeMillis();
    long submittedCount = solr2csv.processAll();
    long end = System.currentTimeMillis();
    long diff = end - start;
    System.out.println( "Exported " + submittedCount + " in " + diff + " ms" );
    //System.out.println( "Reminder: COMMIT_DELAY = " + COMMIT_DELAY + " ms" );

  }


}
