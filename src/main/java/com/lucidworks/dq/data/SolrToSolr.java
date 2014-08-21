package com.lucidworks.dq.data;

import java.io.IOException;
import java.io.PrintWriter;
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

public class SolrToSolr {
  static String HELP_WHAT_IS_IT = "Copy records from one Solr collection or core to another.";
  static String EXTENDED_DESCRIPTION =
    StringUtils.NL
    + "Useful for tasks such as copying data to/from Solr clusters"
    + ", migrating between Solr versions"
    + ", schema debugging"
    + ", or synchronizing Solr instances."
    + " Can ONLY COPY Stored Fields, though this is the default for many fields in Solr."
    + " In syntax messages below, SolrA=source and SolrB=destination."
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

  HttpSolrServer solrA;
  HttpSolrServer solrB;
  int batchSize = DEFAULT_BATCH_SIZE;
  String query = DEFAULT_QUERY;
  boolean ignoreCase = false;
  boolean outputLowercase = false;

  // leave these null by default
  Set<String> includeFields;
  Set<String> includeLiterals;
  Set<Pattern> includePatterns;
  Set<String> excludeFields;
  Set<String> excludeLiterals;
  Set<Pattern> excludePatterns;

  boolean supportsCursorMarks;

  public SolrToSolr( HttpSolrServer source, HttpSolrServer destination ) throws SolrServerException {
    this.solrA = source;
    this.solrB = destination;
    this.supportsCursorMarks = SolrUtils.checkCursorMarkPagingSupport(this.solrA);
    System.out.println( "SolrA source supportsCursorMarks = " + this.supportsCursorMarks );
  }

  public long processAll() throws SolrServerException, IOException {
    SolrQuery q = prepInitialQuery();

    long submittedCount = 0L;
    // Finish up depending on whether we support modern cursors or not
    if (isSupportsCursorMarks() && getBatchSize() > 0) {
      submittedCount = processAllWithCursors(q);
    } else {
      submittedCount = processAllWithOldPaging(q);
    }
    return submittedCount;
  }
  SolrQuery prepInitialQuery() throws SolrServerException {
    // TODO: could put query as fq/filter query to bypass Relevancy sorting
    SolrQuery q = new SolrQuery(getQuery());
    System.out.println( "Fetching rows that match query " + getQuery() );
    // If any restriction on fields, then faster to only fetch fields that we need
    if (null != getIncludeFields() || null != getExcludeFields()) {
      Set<String> storedFields = SolrUtils.getLukeFieldsWithStoredValues(solrA);
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
      QueryResponse rsp = solrA.query( q );
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
      QueryResponse rsp = solrA.query(q);
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
    Collection<SolrInputDocument> destinationDocs = new ArrayList<SolrInputDocument>();
    for ( SolrDocument srcDoc : sourceDocs ) {
      SolrInputDocument dstDoc = convertSolrResultsDocToSolrIndexingDoc( srcDoc );      
      destinationDocs.add( dstDoc );
    }
    solrB.add( destinationDocs, COMMIT_DELAY );
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
      System.out.print( " Submitted " + currentBatchStr + " docs" );
    }
    if ( totalCount > 0L ) {
      System.out.print( " (total = " + totalCountStr + ")" );
    }
    if ( currentBatch <= 0L && totalCount <=0L ) {
      System.out.print( " WARNING: Not passed any stats to log!" );
    }
    System.out.println();
  }
  SolrInputDocument convertSolrResultsDocToSolrIndexingDoc( SolrDocument sourceDoc ) {
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
      if ( isOutputLowercase() ) {
        fieldName = fieldName.toLowerCase();
      }
      destinationDoc.addField( fieldName, fieldValue );
    }
    return destinationDoc;
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
      // If using includes, then it must match
      // If it were a literal field we would have already returned true above
      if ( StringUtils.checkPatternsInList(excludePatterns, fieldName) ) {
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

  public boolean isOutputLowercase() {
    return outputLowercase;
  }
  public void setOutputLowercase(boolean outputLowercase) {
    this.outputLowercase = outputLowercase;
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
    options.addOption( "u", "url_a", true, "URL for SolrA, source of records, OR set host_a (and possibly port_a / collection_a)");
    options.addOption( "h", "host_a", true, "IP address for SolrA, source of records, default=localhost");
    options.addOption( "p", "port_a", true, "Port for SolrA, default=8983");
    options.addOption( "c", "collection_a", true, "Collection/Core for SolrA, Eg: collection1");
    options.addOption( "U", "url_b", true, "URL SolrB, destination of records, OR set host_b (and possibly port_b / collection_b)");
    options.addOption( "H", "host_b", true, "IP address for SolrB, destination of records, default=localhost");
    options.addOption( "P", "port_b", true, "Port for SolrB, default=8983");
    options.addOption( "C", "collection_b", true, "Collection/Core for SolrB, Eg: collection2");

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

    options.addOption( "l", "lowercase_names", false,
            "Change fieldnames to lowercase before submitting to destination collection"
                + "; does NOT affect field name matching."
                + " Note: May create multi-valued fields from previously single-valued fields, Eg: Type=food, type=fruit -> type=[food, fruit]"
                + "; if you see an error about \"multiple values encountered for non multiValued field type\""
                + " this setting can be changed in SolrB's schema.xml file."
                + " There is no output_uppercase since that would complicate the id field."
                + " See also ignore_case");

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
            "Batch size, 1=doc-by-doc."
            + " 0=all-at-once but be careful memory-wise and 0 also disables deep paging cursors."
            + " Default=" + DEFAULT_BATCH_SIZE).hasArg().withType(Number.class) // NOT Long.class
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

    String fullUrlA = cmd.getOptionValue("url_a");
    String hostA = cmd.getOptionValue("host_a");
    String portA = cmd.getOptionValue("port_a");
    String collA = cmd.getOptionValue("collection_a");
    if (null == fullUrlA && null == hostA) {
      helpAndExit( "Must specifify at least url or host for source Solr / SolrA", 3);
    }
    if (null != fullUrlA && null != hostA) {
      helpAndExit( "Must not specifify both url and host for source Solr / SolrA", 4);
    }

    String fullUrlB = cmd.getOptionValue("url_b");
    String hostB = cmd.getOptionValue("host_b");
    String portB = cmd.getOptionValue("port_b");
    String collB = cmd.getOptionValue("collection_b");
    if (null == fullUrlB && null == hostB) {
      helpAndExit( "Must specifify at least url or host for destination Solr / SolrB", 5);
    }
    if (null != fullUrlB && null != hostB) {
      helpAndExit("Must not specifify both url and host for destination Solr / SolrB", 6);
    }

    // Init
    // HttpSolrServer solrA = SolrUtils.getServer( HOST1, PORT1, COLL1 );
    HttpSolrServer solrA;
    if (null != fullUrlA) {
      solrA = SolrUtils.getServer(fullUrlA);
    } else {
      // Utils handle null values
      solrA = SolrUtils.getServer(hostA, portA, collA);
    }
    System.out.println("Source Solr / Solr A = " + solrA.getBaseURL());
    // HttpSolrServer solrB = SolrUtils.getServer( HOST2, PORT2, COLL2 );
    HttpSolrServer solrB;
    if (null != fullUrlB) {
      solrB = SolrUtils.getServer(fullUrlB);
    } else {
      // Utils handle null values
      solrB = SolrUtils.getServer(hostB, portB, collB);
    }
    System.out.println("Destination Solr / Solr B = " + solrB.getBaseURL());

    // Setup older Legacy transport mechanism, if requested
    if (cmd.hasOption("xml")) {
      solrA.setParser(new XMLResponseParser());
      solrB.setParser(new XMLResponseParser());
    }

    // Main instance
    SolrToSolr ss = new SolrToSolr(solrA, solrB);

    // Additional options

    Long batchObj = (Long) cmd.getParsedOptionValue("batch_size");
    if (null != batchObj) {
      if (batchObj.longValue() < 0L) {
        helpAndExit("batch_size must be >= 0", 5);
      }
      ss.setBatchSize(batchObj.intValue());
    }

    // ignore_case
    // Important! Set this BEFORE calling setIncludeFields or setExcludeFields
    // TODO: revisit this, some way to enforce it or do a final check
    if (cmd.hasOption("ignore_case")) {
      ss.setIgnoreCase(true);
    }

    String includeFieldsStr = cmd.getOptionValue("include_fields");
    if (null != includeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(includeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        ss.setIncludeFields(fields);
      }
    }

    String excludeFieldsStr = cmd.getOptionValue("exclude_fields");
    if (null != excludeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(excludeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        ss.setExcludeFields(fields);
      }
    }

    // output_lowercase_field_names
    if (cmd.hasOption("lowercase_names")) {
      ss.setOutputLowercase(true);
    }

    String queryStr = cmd.getOptionValue("query");
    if (null != queryStr) {
      ss.setQuery(queryStr);
    }

    // Do it!
    long start = System.currentTimeMillis();
    long submittedCount = ss.processAll();
    long end = System.currentTimeMillis();
    long diff = end - start;
    System.out.println( "Submitted " + submittedCount + " in " + diff + " ms" );
    System.out.println( "Reminder: COMMIT_DELAY = " + COMMIT_DELAY + " ms" );

    // Batch Size Speed Tests
    // Local to local, 2,108 records, on Solr 4.8.1 w/ cursors
    // batch   = 1           = 13,676 / 12,453 / 12,058 ms
    // batch   = 10          =  2,228 /  2,246 /  2,311 ms
    // batch   = 50          =  1,215 /  1,227 /  1,221 ms
    // batch   = default 100 =  1,130 /  1,163 /  1,113 ms
    // batch   = 250         =    983 /    989 /    985 ms
    // batch   = 500         =    967 /  1,000 /  1,010 ms
    // batch   = 750         =    943 /    906 /    921 ms
    // batch   = 1,000       =    919 /    921 /    937 ms
    // batch 0 = all-at-once =  1,049 /    924 /    980 ms (disables cursors)
  }

}
