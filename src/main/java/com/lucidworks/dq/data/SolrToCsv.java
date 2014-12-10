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
import org.apache.commons.lang.StringEscapeUtils;
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
  // TODO: Could this be done by using wt=csv and cursor-markes?  Would need to map options...

  static String HELP_WHAT_IS_IT = "Export records from Solr collection or core to delimited file, such as CSV.";
  static String EXTENDED_DESCRIPTION =
    StringUtils.NL
    + "Useful for quickly exporting large amounts of data from standalone Solr or SolrCloud to a flat file."
    + " Can ONLY export STORED Fields, though this is the default for many fields in Solr."
    + "May be faster for large result sets than Solr's built-in CSV output, see http://wiki.apache.org/solr/CSVResponseWriter"
    + ", and a bit more convenient."
    + "Has some options for RFC-4180, see https://tools.ietf.org/html/rfc4180"
    + " Will use Solr \"Cursor Marks\", AKA \"Deep Paging\", if available"
    + " which is in Solr version 4.7+"
    + ", see https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results and SOLR-5463"
    + StringUtils.NL
    + "Options:"
    ;
  static String HELP_USAGE = "SolrToCsv --url http://localhost:8983/collection1 --output_file results.csv";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  // See batch size comments at bottom of file
  static int DEFAULT_BATCH_SIZE = 1000;

  public static String DEFAULT_FIELD_DELIMITER = ",";
  public static String DEFAULT_MULTIVALUE_DELIMITER = ", ";
  public static String DEFAULT_RECORD_DELIMITER = "\n";    // RFC-4180 says \r\n
  public static String DEFAULT_NEWLINE_REPLACEMENT = " ";
  
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
  int limitTotalRows;
  int _startOffset;
  String query = DEFAULT_QUERY;
  boolean ignoreCase = false;

  // leave these null by default
  Set<String> includeFields;
  Set<String> includeLiterals;
  Set<Pattern> includePatterns;
  Set<String> excludeFields;
  Set<String> excludeLiterals;
  Set<Pattern> excludePatterns;
  // This is the master list for every output record
  List<String> outputFields;

  boolean supportsCursorMarks;
  private String encodingStr;
  private boolean useStrictEncoding;
  private String outputFile;
  private PrintWriter out;
  private String fieldDelimiter;
  private String recordDelimiter;
  private String multivalueDelimiter;
  private String newlineReplacement;
  private boolean forceQuotes;
  private boolean includeHeader;

  public SolrToCsv( HttpSolrServer source ) throws SolrServerException {
    this.solr = source;
    this.supportsCursorMarks = SolrUtils.checkCursorMarkPagingSupport(this.solr);
    System.out.println( "Solr source supportsCursorMarks = " + this.supportsCursorMarks );
  }

  public long processAll() throws SolrServerException, IOException {
    setupOutputFile();
    calcOutputFields();
    SolrQuery q = prepInitialQuery();

    String header = generateHeaderLine();
    out.write( header );

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

  // We use this for consistent positioning within the CSV records
  // but the search request has slightly different logic and
  // may just request "*", in prepInitialQuery()
  void calcOutputFields() throws SolrServerException {
    outputFields = new ArrayList<>();
    // Give priority order to specific fields they've requested
    if ( null!=includeLiterals ) {
      for ( String candidateField : includeLiterals ) {
        // We DO allow duplicate field names here
        // Odd, but they may have some reason for requesting it
        if ( checkFieldName( candidateField ) ) {
          outputFields.add( candidateField );
        }
      }
    }
    // try to at least get ID first
    else {
      if ( checkFieldName(ID_FIELD) ) {
        outputFields.add( ID_FIELD );
      }
    }
    // Add any others, if applicable
    // TODO: could cache this, in a few cases we might be asking Solr twice for the same info
    // TODO: in other cases, they might only be wanting a couple specific fields
    Set<String> storedFields = SolrUtils.getLukeFieldsWithStoredValues(solr);
    for ( String candidateField : storedFields ) {
      // Don't allow repeats here
      if ( checkFieldName(candidateField) && ! outputFields.contains(candidateField) ) {
        outputFields.add( candidateField );
      }
    }
  }
  SolrQuery prepInitialQuery() throws SolrServerException {
    // TODO: could put query as fq/filter query to bypass Relevancy sorting
    SolrQuery q = new SolrQuery(getQuery());
    System.out.println( "Fetching rows that match query " + getQuery() );
    // If any restriction on fields, then faster to only fetch fields that we need
    // see also slightly different logic in calcOutputFields()
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
        // Limit can be less than one batch-size increment
        if ( getLimitTotalRows() > 0 && docCount >= getLimitTotalRows() ) {
          break;
        }
      }
      processBatch(batch);
      logIntermediateProgressIfApplicable( batch.size(), docCount );

      if (cursorMark.equals(nextCursorMark)) {
        done = true;
      }
      cursorMark = nextCursorMark;

      if ( getLimitTotalRows() > 0 && docCount >= getLimitTotalRows() ) {
        done = true;
      }
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
        // Limit can be less than one batch-size increment
        if ( getLimitTotalRows() > 0 && docCount >= getLimitTotalRows() ) {
          break;
        }
      }
      processBatch(batch);
      logIntermediateProgressIfApplicable( batch.size(), docCount );
      long numFound = docs.getNumFound();
      // done = getBatchSize()<1 || start + getBatchSize() >= numFound;
      done = getBatchSize() < 1 || docCount >= numFound;
      if (getBatchSize() > 0) {
        start += getBatchSize();
      }
      if ( getLimitTotalRows() > 0 && docCount >= getLimitTotalRows() ) {
        done = true;
      }
    }
    return docCount;
  }

  public void processBatch( Collection<SolrDocument> sourceDocs ) throws SolrServerException, IOException {
    if ( null==sourceDocs || sourceDocs.isEmpty() ) {
      return;
    }
    for ( SolrDocument srcDoc : sourceDocs ) {
      outputSolrResultsDocToCsv( srcDoc );
    }
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

  void outputSolrResultsDocToCsv( SolrDocument sourceDoc ) {
    String destDoc = convertSolrResultsDocToCsvString( sourceDoc );
    if ( null!=destDoc ) {
      out.write( destDoc );
    }
  }

  String convertSolrResultsDocToCsvString( SolrDocument sourceDoc ) {
    StringBuffer recordBuff = new StringBuffer();
    // Foreach called-for field
    for ( int i=0; i<outputFields.size(); i++ ) {
      String fieldName = outputFields.get(i);
      if ( i>0 ) {
        recordBuff.append( getFieldDelimiter() );
      }
      Object fieldValue = sourceDoc.getFieldValue( fieldName );
      // Handles nulls, should always return at least ""
      String fieldValueStr = convertSolrFieldToCsvString( fieldValue );
      if ( null!=fieldValueStr && fieldValueStr.length() > 0 ) {
        recordBuff.append( fieldValueStr );
      }
      // TODO: could look for and Warn about any fields returned by Solr that are acceptable but weren't called for here, though that shoudln't be possible
    }
    recordBuff.append( getRecordDelimiter() );
    return new String( recordBuff );
  }

  // Worst case: Outputs an empty string (vs. null)
  // Some options for RFC-4180
  // TODO: option for comma to \, (nonstandard, but Solr's CSV output does it in some cases)
  String convertSolrFieldToCsvString( Object fieldValue ) {
    if ( null==fieldValue ) {
      return "";
    }
    String fieldValueStr = null;
    // Get the raw text
    if ( fieldValue instanceof Collection ) {
      StringBuffer fieldBuff = new StringBuffer();
      Collection<?> values = (Collection) fieldValue;
      for ( Object v : values ) {
        String tmpValueStr = v.toString();
        if ( null!=tmpValueStr && null!=getNewlineReplacement() ) {
          tmpValueStr = tmpValueStr.replaceAll( "\n", getNewlineReplacement() );
        }
        if ( fieldBuff.length() > 0 ) {
          fieldBuff.append( getMultivalueDelimiter() );
        }
        fieldBuff.append( tmpValueStr );
      }
      fieldValueStr = new String( fieldBuff );
    }
    else {
      String tmpValueStr = fieldValue.toString();
      if ( null!=tmpValueStr && null!=getNewlineReplacement() ) {
        tmpValueStr = tmpValueStr.replaceAll( "\n", getNewlineReplacement() );
      }
      fieldValueStr = tmpValueStr;
    }
    // be paranoid
    if ( null==fieldValueStr ) {
      fieldValueStr = "";
    }

    // Cleanup and Escaping
    // Various reasons to need quotes
    boolean needsQuotes = false;

    // Newline check
    if ( fieldValueStr.indexOf('\n') > 0 ) {
      needsQuotes = true;
    }

    // Comma check
    if ( fieldValueStr.indexOf( getFieldDelimiter() ) > 0 ) {
      needsQuotes = true;
    }

    // Handle embedded quotes
    String origString = fieldValueStr;
    fieldValueStr = fieldValueStr.replaceAll( "\"", "\"\"" );
    if ( ! fieldValueStr.equals(origString) ) {
      needsQuotes = true;
    }

    // Foce quotes option overrides everything
    needsQuotes |= getForceQuotes();

    if ( needsQuotes ) {
      return "\"" + fieldValueStr + "\"";
    }
    else {
      return fieldValueStr;
    }
  }

  String generateHeaderLine() {
    StringBuffer recordBuff = new StringBuffer();
    // Foreach called-for field
    for ( int i=0; i<outputFields.size(); i++ ) {
      String fieldName = outputFields.get(i);
      if ( i>0 ) {
        recordBuff.append( getFieldDelimiter() );
      }
      String fieldValueStr = convertSolrFieldToCsvString( fieldName );
      if ( null!=fieldValueStr && fieldValueStr.length() > 0 ) {
        recordBuff.append( fieldValueStr );
      }
    }
    recordBuff.append( getRecordDelimiter() );
    return new String( recordBuff );
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

  public int getLimitTotalRows() {
    return limitTotalRows;
  }
  public void setLimitTotalRows(int numberOfRows) {
    this.limitTotalRows = numberOfRows;
  }
  public int _getStartOffset() {
    return _startOffset;
  }
  public void _setStartOffset(int offset) {
    this._startOffset = offset;
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

  public void setFieldDelimiter(String delimiter) {
    this.fieldDelimiter = delimiter;
  }
  public String getFieldDelimiter() {
    return this.fieldDelimiter;
  }
  public void setRecordDelimiter(String delimiter) {
    this.recordDelimiter = delimiter;
  }
  public String getRecordDelimiter() {
    return this.recordDelimiter;
  }
  public void setMultivalueDelimiter(String delimiter) {
    this.multivalueDelimiter = delimiter;
  }
  public String getMultivalueDelimiter() {
    return this.multivalueDelimiter;
  }
  public void setNewlineReplacement(String substStr) {
    this.newlineReplacement = substStr;
  }
  public String getNewlineReplacement() {
    return this.newlineReplacement;
  }

  public void setForceQuotes(boolean flag) {
    this.forceQuotes = flag;
  }
  public boolean getForceQuotes() {
    return this.forceQuotes;
  }

  public void setIncludeHeader(boolean flag) {
    this.includeHeader = flag;
  }
  public boolean getIncludeHeader() {
    return this.includeHeader;
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

    options.addOption( "x", "xml", false,
        "Use XML transport (XMLResponseParser) instead of default javabin; useful when working with older versions of Solr, though slightly slower."
            + " Helps fix errors \"RuntimeException: Invalid version or the data in not in 'javabin' format\""
            + ", \"org.apache.solr.common.util.JavaBinCodec.unmarshal\", or similar errors.");

    options.addOption( "q", "query", true, "Query to select which records will be copied; by default all records are copied.");

    options.addOption(
        OptionBuilder.withLongOpt("batch_size")
          .withDescription(
            "Batch size from Solr, 1=doc-by-doc."
            + " 0=all-at-once but be careful memory-wise and 0 also disables deep paging cursors."
            + " Default=" + DEFAULT_BATCH_SIZE
            + " Does NOT affect size of output data file, which just puts everything into one giant file (in this early version)."
            ).hasArg().withType(Number.class) // NOT Long.class
          .create("b"));

    // rows and start
    options.addOption(
        OptionBuilder
          .withLongOpt("rows")
          .withDescription( "Limit total number of rows to export, useful for testing small batches." )  // See also --start" )
          .hasArg()
          .withType(Number.class) // NOT Long.class
          .create()
          );
//    options.addOption(
//        OptionBuilder
//          .withLongOpt("_start")
//          .withDescription( "Offset of row to start exporting from, useful for testing small batches. See also --rows" )
//          .hasArg()
//          .withType(Number.class) // NOT Long.class
//          .create()
//          );

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

    options.addOption( "o", "output_file", true, "Output file to export data to (default or \"-\" is stdout / standard out)" );
    options.addOption( "e", "encoding", true, "Character Encoding for writing files (default is UTF-8, which enables cross-platform operation)" );
    options.addOption( "l", "loose_encoding", false, "Disable strict character encoding so that problems don't throw Exceptions (NOT recommended)" );

    options.addOption( "d", "field_delimiter", true, "Field separator for output records"
        + ", supports Java escape sequences but be careful with your OS shell."
        + " For example, for a TAB, you can try \"\\\\t\" or \"\\t\" "
        + " Default is \"" + DEFAULT_FIELD_DELIMITER + "\"" );
    options.addOption( "m", "multivalue_delimiter", true, "Separator for individual multi-value fields"
        + ", supports Java escape sequences but be careful with your OS shell."
        + " Default is \"" + DEFAULT_MULTIVALUE_DELIMITER + "\"" );
    options.addOption( "r", "record_delimiter", true, "Separator for complete records"
        + ", supports Java escape sequences but be careful with your OS shell."
        + " Default is newline"
        + ", although RFC-4180 suggests using \"\\r\\n\""
        + ", set with either \"\\r\\n\" or \"\\\\r\\\\n\""
        );
    options.addOption( "n", "newline_replacement", true,
        "If a newline appears within a value, what should it be replaced with."
        + " Default is \"" + DEFAULT_NEWLINE_REPLACEMENT + "\""
        + ", although RFC-4180 suggests using a bare newline which you can set with \"\\n\""
        + " To get an actual \"\\n\" try using \"\\\\n\" or \"\\\\\\\\n\""
        + " Does not replace newlines added via multivalue_delimiter"
        + "; if you don't want those either, then change that other setting."
        );

    options.addOption( "s", "skip_header", false,
        "Normally we output the field names as the first line of the output CSV; turn this feature off." );
    options.addOption( "f", "force_quotes", false,
        "Normally only values with commas, quotes or newlines are wrapped with quotes; this forces all values to be quoted." );

    // TODO: start at a certain offset (would be difficult)
    // TODO: sort options?
    // TODO: Escape Commas option, currently we don't do it, just add quotes

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

    // Batch size (retrieval)
    Long batchObj = (Long) cmd.getParsedOptionValue("batch_size");
    if (null != batchObj) {
      if (batchObj.longValue() < 0L) {
        helpAndExit("batch_size must be >= 0", 5);
      }
      solr2csv.setBatchSize(batchObj.intValue());
    }

    // Limit output rows, handy for debugging
    int rows = 0;
    // Don't use Integer
    Long rowsObj = (Long) cmd.getParsedOptionValue("rows");
    if (null != rowsObj) {
      if (rowsObj.intValue() <= 0) {
        helpAndExit("rows must be > 0", 5);
      }
      rows = rowsObj.intValue();
      solr2csv.setLimitTotalRows( rows );
    }

//    // Won't work with cursors
//    // and complication even with older batching
//    int start = 0;
//    // Don't use Integer
//    Long startObj = (Long) cmd.getParsedOptionValue("start");
//    if (null != startObj) {
//      if (startObj.intValue() <= 0) {
//        helpAndExit("start must be > 0", 6);
//      }
//      start = startObj.intValue();
//      solr2csv._setStartOffset( start );
//    }

    // ignore_case for field matching
    // Important! Set this BEFORE calling setIncludeFields or setExcludeFields
    // TODO: revisit this, some way to enforce it or do a final check
    if (cmd.hasOption("ignore_case")) {
      solr2csv.setIgnoreCase(true);
    }

    // Include Fields and Patterns
    String includeFieldsStr = cmd.getOptionValue("include_fields");
    if (null != includeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(includeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        solr2csv.setIncludeFields(fields);
      }
    }

    // Exclude Fields and Patterns
    String excludeFieldsStr = cmd.getOptionValue("exclude_fields");
    if (null != excludeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(excludeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        solr2csv.setExcludeFields(fields);
      }
    }

    // Field Delim, usually Comma or Tab
    String fieldDelim = cmd.getOptionValue("field_delimiter");
    if (null != fieldDelim) {
      fieldDelim = StringEscapeUtils.unescapeJava( fieldDelim );
      solr2csv.setFieldDelimiter( fieldDelim );
    }
    else {
      solr2csv.setFieldDelimiter( DEFAULT_FIELD_DELIMITER );
    }

    // Multivalue Field Delim
    String mvDelim = cmd.getOptionValue("multivalue_delimiter");
    if (null != mvDelim) {
      mvDelim = StringEscapeUtils.unescapeJava( mvDelim );
      solr2csv.setMultivalueDelimiter( mvDelim );
    }
    else {
      solr2csv.setMultivalueDelimiter( DEFAULT_MULTIVALUE_DELIMITER );
    }

    // Record Delim, usually newline
    String recordDelim = cmd.getOptionValue("record_delimiter");
    if (null != recordDelim) {
      recordDelim = StringEscapeUtils.unescapeJava( recordDelim );
      solr2csv.setRecordDelimiter( recordDelim );
    }
    else {
      solr2csv.setRecordDelimiter( DEFAULT_RECORD_DELIMITER );
    }

    // Newline Replacement
    String newline = cmd.getOptionValue("newline_replacement");
    if (null != newline) {
      newline = StringEscapeUtils.unescapeJava( newline );
      solr2csv.setNewlineReplacement( newline );
    }
    else {
      solr2csv.setNewlineReplacement( DEFAULT_NEWLINE_REPLACEMENT );
    }

    // Force Quotes
    if (cmd.hasOption("force_quotes")) {
      solr2csv.setForceQuotes(true);
    }

    // Include Header field names or not
    // Note: reverse logic here, skip vs. include
    if (cmd.hasOption("skip_header")) {
      solr2csv.setIncludeHeader(false);
    }
    else {
      solr2csv.setIncludeHeader(true);
    }

    // Query, limit records
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

    
    // Do it!
    long startTime = System.currentTimeMillis();
    long submittedCount = solr2csv.processAll();
    long endTime = System.currentTimeMillis();
    long diffTime = endTime - startTime;
    System.out.println( "Exported " + submittedCount + " in " + diffTime + " ms" );
    //System.out.println( "Reminder: COMMIT_DELAY = " + COMMIT_DELAY + " ms" );

  }


}
