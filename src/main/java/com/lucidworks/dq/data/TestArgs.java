package com.lucidworks.dq.data;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.StringUtils;

public class TestArgs {
  
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
  
  static String HELP_WHAT_IS_IT = "TestArgs";
  static String HELP_USAGE = "(usage)";
  static void helpAndExit() {
    helpAndExit( null, 1 );
  }
  static void helpAndExit( String optionalError, int errorCode ) {
    HelpFormatter formatter = new HelpFormatter();
    if ( null==optionalError ) {
      // log.info( HELP_WHAT_IS_IT );
      System.out.println( HELP_WHAT_IS_IT );
    }
    else {
      // log.error( optionalError );
      System.err.println( optionalError );
    }
    //formatter.printHelp( HELP_USAGE, options, true );
    System.exit( errorCode );
  }

  public static void main(String[] args) throws ParseException {
    // TODO Auto-generated method stub

    // Good args from SolrToSolr
    String[] goodArgs = { "-h", "localhost", "-c", "salesforce_accounts_temp",
                          "-H", "localhost", "-C", "salesforce_accounts",
                          "-exclude_fields", "timestamp,text_en",
                          "--batch_size", "500" };
    // Good args from SolrToSolr
    Options goodOptions;

    // Bad from EmptyFieldStats
    String[] badArgs = { "--host", "localhost", "--collection", "search-master",
                         "--stored_fields", "--rows", "1000" };
    // Bad from EmptyFieldStats
    Options badOptions;


    // Good from SolrToSolr
    goodOptions = new Options();
    goodOptions.addOption( "u", "url_a", true, "URL for SolrA, source of records, OR set host_a (and possibly port_a / collection_a)");
    goodOptions.addOption( "h", "host_a", true, "IP address for SolrA, source of records, default=localhost");
    goodOptions.addOption( "p", "port_a", true, "Port for SolrA, default=8983");
    goodOptions.addOption( "c", "collection_a", true, "Collection/Core for SolrA, Eg: collection1");
    goodOptions.addOption( "U", "url_b", true, "URL SolrB, destination of records, OR set host_b (and possibly port_b / collection_b)");
    goodOptions.addOption( "H", "host_b", true, "IP address for SolrB, destination of records, default=localhost");
    goodOptions.addOption( "P", "port_b", true, "Port for SolrB, default=8983");
    goodOptions.addOption( "C", "collection_b", true, "Collection/Core for SolrB, Eg: collection2");

    goodOptions.addOption( "q", "query", true, "Query to select which records will be copied; by default all records are copied.");

    // Bad from EmptyFieldStats
    badOptions = new Options();
    badOptions.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
    badOptions.addOption( "h", "host", true, "IP address for Solr, default=localhost" );
    badOptions.addOption( "p", "port", true, "Port for Solr, default=8983" );
    badOptions.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
    badOptions.addOption( "s", "stored_fields", false, "Also check stats of Stored fields. WARNING: may take lots of time and memory for large collections" );
    badOptions.addOption( "i", "ids", false, "Include IDs of docs with empty fields. WARNING: may create large report" );
    // TODO: could add option for number of IDs to include...
    badOptions.addOption( "f", "fields", true, "Fields to analyze, Eg: fields=name,category, default is all fields" );


    // Good from SolrToSolr
    goodOptions.addOption( "f", "include_fields", true,
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
    goodOptions.addOption( "F", "exclude_fields", true,
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

    goodOptions.addOption( "i", "ignore_case", false,
            "Ignore UPPER and lowercase differences when matching field names and patterns, AKA case insensitive"
                + "; the original form of the fieldname will still be output to the destination collection"
                + " unless output_lowercase_names is used");

    goodOptions.addOption( "l", "lowercase_names", false,
            "Change fieldnames to lowercase before submitting to destination collection"
                + "; does NOT affect field name matching."
                + " Note: May create multi-valued fields from previously single-valued fields, Eg: Type=food, type=fruit -> type=[food, fruit]"
                + "; if you see an error about \"multiple values encountered for non multiValued field type\""
                + " this setting can be changed in SolrB's schema.xml file."
                + " There is no output_uppercase since that would complicate the id field."
                + " See also ignore_case");

    goodOptions.addOption( "x", "xml", false,
            "Use XML transport (XMLResponseParser) instead of default javabin; useful when working with older versions of Solr, though slightly slower."
                + " Helps fix errors \"RuntimeException: Invalid version or the data in not in 'javabin' format\""
                + ", \"org.apache.solr.common.util.JavaBinCodec.unmarshal\", or similar errors.");

    // TODO: Multivalue options?
    // TODO: start at a certain offset or limit total number of records
    // TODO: sort options?
    // TODO: commit options

    // Good from SolrToSolr
    goodOptions.addOption(
        OptionBuilder
          .withLongOpt("batch_size")
          .withDescription(
            "Batch size, 1=doc-by-doc."
            + " 0=all-at-once but be careful memory-wise and 0 also disables deep paging cursors."
            + " Default=" + DEFAULT_BATCH_SIZE)
           .hasArg()
           .withType(Number.class) // NOT Long.class
          .create("b")
          );

    // rows and start

    // Bad from EmptyFieldStats
    // Bad v0
//    badOptions.addOption(
//        OptionBuilder
//          .withLongOpt("rows")
//          .withDescription( "Limit number of rows to check for stored values; useful for low memory, see also start" )
//          .create()
//          );
    // Bad v1?
    badOptions.addOption(
        OptionBuilder
          .withLongOpt("rows")
          .withDescription( "Limit number of rows to check for stored values; useful for low memory, see also start" )
          // add, start
           .hasArg()
           .withType(Number.class) // NOT Long.class
          // add, end
          .create()
          );

    // Bad from EmptyFieldStats
    badOptions.addOption(
        OptionBuilder.withLongOpt("start")
          .withDescription( "Offset of row to start checking for stored values; useful for low memory, see also rows" )
          .create());

    
    

    
    // Good from SolrToSolr
    if (goodArgs.length < 1) {
      helpAndExit();
    }
    CommandLine goodCmd = null;
    try {
      CommandLineParser parser = new PosixParser();
      goodCmd = parser.parse(goodOptions, goodArgs);
    } catch (ParseException exp) {
      helpAndExit("Parsing command line failed. Reason: " + exp.getMessage(), 2);
    }

    // Bad from EmptyFieldStats
    if ( badArgs.length < 1 ) {
      helpAndExit();
    }
    CommandLine badCmd = null;
    try {
      CommandLineParser parser = new PosixParser();
      // CommandLineParser parser = new DefaultParser();
      badCmd = parser.parse( badOptions, badArgs );
    }
    catch( ParseException exp ) {
      helpAndExit( "Parsing command line failed. Reason: " + exp.getMessage(), 2 );
    }
    
    
    
    
    // Good from SolrToSolr
    String fullUrlA = goodCmd.getOptionValue("url_a");
    String hostA = goodCmd.getOptionValue("host_a");
    String portA = goodCmd.getOptionValue("port_a");
    String collA = goodCmd.getOptionValue("collection_a");
    if (null == fullUrlA && null == hostA) {
      helpAndExit( "Must specifify at least url or host for source Solr / SolrA", 3);
    }
    if (null != fullUrlA && null != hostA) {
      helpAndExit( "Must not specifify both url and host for source Solr / SolrA", 4);
    }

    String fullUrlB = goodCmd.getOptionValue("url_b");
    String hostB = goodCmd.getOptionValue("host_b");
    String portB = goodCmd.getOptionValue("port_b");
    String collB = goodCmd.getOptionValue("collection_b");
    if (null == fullUrlB && null == hostB) {
      helpAndExit( "Must specifify at least url or host for destination Solr / SolrB", 5);
    }
    if (null != fullUrlB && null != hostB) {
      helpAndExit("Must not specifify both url and host for destination Solr / SolrB", 6);
    }

//    // Init
//    // HttpSolrServer solrA = SolrUtils.getServer( HOST1, PORT1, COLL1 );
//    HttpSolrServer solrA;
//    if (null != fullUrlA) {
//      solrA = SolrUtils.getServer(fullUrlA);
//    } else {
//      // Utils handle null values
//      solrA = SolrUtils.getServer(hostA, portA, collA);
//    }
//    System.out.println("Source Solr / Solr A = " + solrA.getBaseURL());
//    // HttpSolrServer solrB = SolrUtils.getServer( HOST2, PORT2, COLL2 );
//    HttpSolrServer solrB;
//    if (null != fullUrlB) {
//      solrB = SolrUtils.getServer(fullUrlB);
//    } else {
//      // Utils handle null values
//      solrB = SolrUtils.getServer(hostB, portB, collB);
//    }
//    System.out.println("Destination Solr / Solr B = " + solrB.getBaseURL());


    // Bad from EmptyFieldStats
    // Already using -h for host, don't really need help, just run with no options
    //if ( cmd.hasOption("help") ) {
    //  helpAndExit();
    //}
    String fullUrl = badCmd.getOptionValue( "url" );
    String host = badCmd.getOptionValue( "host" );
    String port = badCmd.getOptionValue( "port" );
    String coll = badCmd.getOptionValue( "collection" );
    if ( null==fullUrl && null==host ) {
      helpAndExit( "Must specifify at least url or host", 3 );
    }
    if ( null!=fullUrl && null!=host ) {
      helpAndExit( "Must not specifify both url and host", 4 );
    }
//    // Init
//    // HttpSolrServer solr = SolrUtils.getServer( HOST, PORT, COLL );
//    HttpSolrServer solr;
//    if ( null!=fullUrl ) {
//      solr = SolrUtils.getServer( fullUrl );
//    }
//    else {
//      // Utils handle null values
//      solr = SolrUtils.getServer( host, port, coll );    
//    }

    
    
    // Good from SolrToSolr
    // Setup older Legacy transport mechanism, if requested
    if (goodCmd.hasOption("xml")) {
//      solrA.setParser(new XMLResponseParser());
//      solrB.setParser(new XMLResponseParser());
    }


    

    // ignore_case
    // Important! Set this BEFORE calling setIncludeFields or setExcludeFields
    // TODO: revisit this, some way to enforce it or do a final check
    if (goodCmd.hasOption("ignore_case")) {
      //ss.setIgnoreCase(true);
    }

    String includeFieldsStr = goodCmd.getOptionValue("include_fields");
    if (null != includeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(includeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        //ss.setIncludeFields(fields);
      }
    }

    String excludeFieldsStr = goodCmd.getOptionValue("exclude_fields");
    if (null != excludeFieldsStr) {
      Set<String> fields = StringUtils.splitCsv(excludeFieldsStr);
      if (null != fields && !fields.isEmpty()) {
        //ss.setExcludeFields(fields);
      }
    }

    // output_lowercase_field_names
    if (goodCmd.hasOption("lowercase_names")) {
      //ss.setOutputLowercase(true);
    }

    String queryStr = goodCmd.getOptionValue("query");
    if (null != queryStr) {
      //ss.setQuery(queryStr);
    }
    
    
    // Bad from EmptyFieldStats
    // Options
    boolean includeStoredFields = false;
    if(badCmd.hasOption("stored_fields")) {
      includeStoredFields = true;
    }
    boolean showIds = false;
    if(badCmd.hasOption("ids")) {
      showIds = true;
    }
    Set<String> targetFields = null;
    String fieldsStr = badCmd.getOptionValue( "fields" );
    if ( null!=fieldsStr ) {
      Set<String> fields = SetUtils.splitCsv( fieldsStr );
      if ( ! fields.isEmpty() ) {
        targetFields = fields;
      }
    }

    
    // Good from SolrToSolr
    Long tmpObj = (Long) goodCmd.getParsedOptionValue("batch_size");

    // Main instance
//    SolrToSolr ss = new SolrToSolr(solrA, solrB);

    // Additional options

    Long batchObj = (Long) goodCmd.getParsedOptionValue("batch_size");
    if (null != batchObj) {
      if (batchObj.longValue() < 0L) {
        helpAndExit("batch_size must be >= 0", 5);
      }
      //ss.setBatchSize(batchObj.intValue());
    }

    // Bad from EmptyFieldStats
    int rows = 0;
    // Integer rowsObj = (Integer) badCmd.getParsedOptionValue("rows");
    // java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Integer
    Long rowsObj = (Long) badCmd.getParsedOptionValue("rows");
    if (null != rowsObj) {
      if (rowsObj.intValue() <= 0) {
        helpAndExit("rows must be > 0", 5);
      }
      rows = rowsObj.intValue();
    }

    // Bad-ish from EmptyFieldStats
    int start = 0;
    Integer startObj = (Integer) badCmd.getParsedOptionValue("start");
    if (null != startObj) {
      if (startObj.intValue() <= 0) {
        helpAndExit("start must be > 0", 6);
      }
      start = startObj.intValue();
    }
    
    
    
    
    
  }

}
