package com.lucidworks.dq.schema;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
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
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.UpdateRequestHandler;

import com.lucidworks.dq.util.DateUtils;
import com.lucidworks.dq.util.IO_Utils;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;
import com.lucidworks.dq.util.StringUtils;

public class SchemalessPlus {
  static String HELP_WHAT_IS_IT = "Guess data types based on multiple Solr documents.";
  static String EXTENDED_DESCRIPTION = "tries to extend schemaless";
  static String HELP_USAGE = "SchemalessPlus --url_a http://localhost:8983/solr/collection1";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }


  static Options options;

  HttpSolrServer solrA;

  public SchemalessPlus( HttpSolrServer source )
      throws SolrServerException {
    this.solrA = source;
  }

  SolrInputDocument convertSolrResultsDocToSolrIndexingDoc( SolrDocument sourceDoc ) {
//    SolrInputDocument destinationDoc = new SolrInputDocument();
//    for( Entry<String, Object> srcEntry : sourceDoc.entrySet() ) {
//      String fieldName = srcEntry.getKey();
//      // Skip internal fields like _version_
//      // We still need to check fields here
//      // Although we call filterFields when preparing the query
//      // sometimes we just use fl=* so need to check here
//      if ( ! checkFieldName(fieldName) ) {
//        continue;
//      }
//      Object fieldValue = srcEntry.getValue();
//      if ( isOutputLowercase() ) {
//        fieldName = fieldName.toLowerCase();
//      }
//      destinationDoc.addField( fieldName, fieldValue );
//    }
//    return destinationDoc;
    return null;
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

  static String calcSolrXML( String host, int port ) {
    return
      "<solr persistent=\"false\"> \n"
    + "  <cores adminPath=\"/admin/cores\" \n"
    + "    defaultCoreName=\"collection1\" \n"
    + "    host=\"" + host + "\" \n"
    + "    hostPort=\"" + port + "\" \n"
    + "  />\n"
    + "</solr> \n";
  }
  // String solrxml = "<solr><str name=\"configSetBaseDir\">${configsets:configsets}</str></solr>";
  
  public static void testCore() throws IOException, URISyntaxException {
    //String solrHome = "/Users/mbennett/data/dev/solr-lucene-490-src/solr/example/solr";
    String solrHome = IO_Utils.materializeSolrHomeIntoTemp().toString();
    
    System.setProperty( "solr.solr.home", solrHome );
    String solrXML = calcSolrXML( "localhost", 8984 );
    SolrResourceLoader loader = new SolrResourceLoader(SolrResourceLoader.locateSolrHome());
    ConfigSolr cfg = ConfigSolr.fromString( loader, solrXML );
    CoreContainer container = new CoreContainer(loader, cfg);
    container.load();

    System.out.println( "Container = " + container );

    //UpdateRequestHandler updater = new UpdateRequestHandler();
    //updater.init( null );

    // TODO: from core container to solr instance I can submit commands to?
    String testDirectory = container.getResourceLoader().getInstanceDir();
    System.out.println( "testDir = " + testDirectory );
    
    String tempCoreName = "_temp_";
    String configSetName = "solr_490_schemaless_ramdir";
    
//    CoreAdminRequest request = new CoreAdminRequest();
//    request.setAction(CoreAdminAction.CREATE);
//    request.setCoreName( "_temp_" );
//    request.setInstanceDir("."); 
//    request.setDataDir(".");
    CoreAdminRequest.Create createCmd = new CoreAdminRequest.Create(); 
    createCmd.setCoreName( tempCoreName );
    createCmd.setConfigSet( configSetName );
    //createCmd.setInstanceDir( "." );
    //createCmd.setDataDir( "." );

    createCmd.setIsTransient( true );
    createCmd.setIsLoadOnStartup( false );

    //createCmd.setCollectionConfigName( solrConfig.getCanonicalPath() );

    // Invalid path string "/configs//Users/mbennett/data/dev/DQ/data-quality-github/src/main/resources/conf-490-schemaless-ramdir/solrconfig.xml"
    // caused by empty node name specified @9"

    // Can't find resource 'solrconfig.xml' in classpath
    // or '/Users/mbennett/data/dev/solr-4.9.0-nested-docs/example/solr/_temp_/conf'

    // createCmd.setSchemaName( schema.getCanonicalPath() );

    // org.apache.solr.common.cloud.ZooKeeperException:org.apache.solr.common.cloud.ZooKeeperException:
    // Could not find configName for collection _temp2_ 
    
    //CoreAdminResponse result = createCmd.process( coreAdminServerA ); 
    //System.out.println( "result = " + result );

    
  }
  
  public static void main(String[] argv) throws ParseException,
      SolrServerException, IOException, URISyntaxException
  {
    
    testCore();

/***
    options = new Options();
    options.addOption( "u", "url_a", true, "URL for SolrA, source of records, OR set host_a (and possibly port_a / collection_a)");
    options.addOption( "h", "host_a", true, "IP address for SolrA, source of records, default=localhost");
    options.addOption( "p", "port_a", true, "Port for SolrA, default=8983");
    options.addOption( "c", "collection_a", true, "Collection/Core for SolrA, Eg: collection1");
//    options.addOption( "U", "url_b", true, "URL SolrB, destination of records, OR set host_b (and possibly port_b / collection_b)");
//    options.addOption( "H", "host_b", true, "IP address for SolrB, destination of records, default=localhost");
//    options.addOption( "P", "port_b", true, "Port for SolrB, default=8983");
//    options.addOption( "C", "collection_b", true, "Collection/Core for SolrB, Eg: collection2");

    options.addOption( "t", "template_conf_dir", true, "Filesystem path to a conf directory configured for both Schemaless and RAMDirectory");

//    options.addOption( "q", "query", true, "Query to select which records will be copied; by default all records are copied.");

//    options.addOption( "f", "include_fields", true,
//            "Fields to copy, Eg: include_fields=id,name,category"
//                + " Make sure to include the id!"
//                + " By default all stored fields are included except system fields like _version_ and _root_."
//                + " You can also use simple globbing patterns like billing_* but"
//                + " make sure to use quotes on the command line to protect them from the operating system."
//                + " Field name and pattern matching IS case sensitive unless you set ignore_case."
//                + " Patterns do NOT match system fields either, so if really need a field like _version_"
//                + " then add the full name to include_fields not using a wildcard."
//                + " Solr field names should not contain commas, spaces or wildcard pattern characters."
//                + " Does not use quite the same rules as dynamicField pattern matching, different implementation."
//                + " See also exclude_fields");
//    options.addOption( "F", "exclude_fields", true,
//            "Fields to NOT copy over, Eg: exclude_fields=timestamp,text_en"
//                + " Useful for skipping fields that will be re-populated by copyField in SolrB schema.xml."
//                + " System fields like _version_ are already skipped by default."
//                + " Use literal field names or simple globbing patterns like text_*"
//                + "; remember to use quotes on the command line to protect wildcard characters from the operating system."
//                + " Excludes override includes when comparing literal field names or when comparing patterns"
//                + ", except that literal fields always take precedence over patterns."
//                + " If a literal field name appears in both include and exclude, it will not be included."
//                + " If a field matches both include and exclude patterns, it will not be included."
//                + " However, if a field appears as a literal include but also happens to match an exclude pattern"
//                + ", then the literal reference will win and it WILL be included."
//                + " See also include_fields");

//    options.addOption( "i", "ignore_case", false,
//            "Ignore UPPER and lowercase differences when matching field names and patterns, AKA case insensitive"
//                + "; the original form of the fieldname will still be output to the destination collection"
//                //+ " unless output_lowercase_names is used"
//                );

//    options.addOption( "l", "lowercase_names", false,
//            "Change fieldnames to lowercase before submitting to destination collection"
//                + "; does NOT affect field name matching."
//                + " Note: May create multi-valued fields from previously single-valued fields, Eg: Type=food, type=fruit -> type=[food, fruit]"
//                + "; if you see an error about \"multiple values encountered for non multiValued field type\""
//                + " this setting can be changed in SolrB's schema.xml file."
//                + " There is no output_uppercase since that would complicate the id field."
//                + " See also ignore_case");

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

//    String fullUrlB = cmd.getOptionValue("url_b");
//    String hostB = cmd.getOptionValue("host_b");
//    String portB = cmd.getOptionValue("port_b");
//    String collB = cmd.getOptionValue("collection_b");
//    if (null == fullUrlB && null == hostB) {
//      helpAndExit( "Must specifify at least url or host for destination Solr / SolrB", 5);
//    }
//    if (null != fullUrlB && null != hostB) {
//      helpAndExit("Must not specifify both url and host for destination Solr / SolrB", 6);
//    }

    // Init
    // TODO: this logic only works if solr running under container with "/solr" in path, should be more flexible
    // BASE Solr URL
    HttpSolrServer coreAdminServerA;
    // URL with collection, if any
    HttpSolrServer collectionServerA;
    if (null != fullUrlA) {
      collectionServerA = SolrUtils.getServer(fullUrlA);
      // Using default collection, that's good
      if ( fullUrlA.endsWith("/solr") || fullUrlA.endsWith("/solr/") ) {
        coreAdminServerA = SolrUtils.getServer(fullUrlA);
      }
      // Added collection name to end of URL
      else if ( fullUrlA.indexOf("/solr/") > 0 ) {
        int endsAt = fullUrlA.lastIndexOf("/solr/") + "/solr/".length();
        String shortUrl = fullUrlA.substring( 0, endsAt );
        coreAdminServerA = SolrUtils.getServer(shortUrl);
      }
      else {
        System.out.println( "WARNING: Solr URL doesn't include \"/solr\" in path; assuming use of *default* collection config, such that Collection URL and Core Admin URL are the same" );
        coreAdminServerA = SolrUtils.getServer(fullUrlA);
        collectionServerA = SolrUtils.getServer(fullUrlA); 
      }
    }
    // Else didn't give us a full url
    else {
      // Utils handle null values
      coreAdminServerA = SolrUtils.getServer(hostA, portA);
      collectionServerA = SolrUtils.getServer(hostA, portA, collA);
    }
//    System.out.println("Source Solr / Solr A = " + solrA.getBaseURL());
    System.out.println("Solr Core Admin URL = " + coreAdminServerA.getBaseURL() );
    System.out.println("Solr Collection URL = " + collectionServerA.getBaseURL() );
//    // HttpSolrServer solrB = SolrUtils.getServer( HOST2, PORT2, COLL2 );
//    HttpSolrServer solrB;
//    if (null != fullUrlB) {
//      solrB = SolrUtils.getServer(fullUrlB);
//    } else {
//      // Utils handle null values
//      solrB = SolrUtils.getServer(hostB, portB, collB);
//    }
//    System.out.println("Destination Solr / Solr B = " + solrB.getBaseURL());

    // Setup older Legacy transport mechanism, if requested
    if (cmd.hasOption("xml")) {
      coreAdminServerA.setParser(new XMLResponseParser());
      collectionServerA.setParser(new XMLResponseParser());
//      solrB.setParser(new XMLResponseParser());
    }
    
    Set<String> coreNames = SolrUtils.getCoreNames( coreAdminServerA );
    System.out.println( "Core Names: " + coreNames );

    // template_conf_path
    String confPath = cmd.getOptionValue("template_conf_dir");
    File confDir = new File( confPath );
    File schema = new File( confDir, "schema.xml" );
    if ( ! schema.exists() || ! schema.canRead() || ! schema.isFile() ) {
      throw new IOException( "Issue with schema file \"" + schema.getCanonicalPath() + "\""
          + ", derived from template_conf_dir \"" + confPath + "\""
          + " - Check that this *file* exists and is readable."
          );
    }
    System.out.println( "Schema: " + schema.getCanonicalPath() );
    File solrConfig = new File( confDir, "solrconfig.xml" );
    if ( ! solrConfig.exists() || ! solrConfig.canRead() || ! solrConfig.isFile() ) {
      throw new IOException( "Issue with solrconfig file \"" + solrConfig.getCanonicalPath() + "\""
          + ", derived from template_conf_dir \"" + confPath + "\""
          + " - Check that this *file* exists and is readable."
          );
    }
    System.out.println( "Solrconfig: " + solrConfig.getCanonicalPath() );

***/
    
    
//    // Main instance
//    //SchemalessPlus sp = new SchemalessPlus(solrA, solrB);
//    SchemalessPlus sp = new SchemalessPlus(solrA);
//
//    // Additional options
//
//    Long batchObj = (Long) cmd.getParsedOptionValue("batch_size");
//    if (null != batchObj) {
//      if (batchObj.longValue() < 0L) {
//        helpAndExit("batch_size must be >= 0", 5);
//      }
//      sp.setBatchSize(batchObj.intValue());
//    }
//
//    // ignore_case
//    // Important! Set this BEFORE calling setIncludeFields or setExcludeFields
//    // TODO: revisit this, some way to enforce it or do a final check
//    if (cmd.hasOption("ignore_case")) {
//      sp.setIgnoreCase(true);
//    }
//
//    String includeFieldsStr = cmd.getOptionValue("include_fields");
//    if (null != includeFieldsStr) {
//      Set<String> fields = StringUtils.splitCsv(includeFieldsStr);
//      if (null != fields && !fields.isEmpty()) {
//        sp.setIncludeFields(fields);
//      }
//    }
//
//    String excludeFieldsStr = cmd.getOptionValue("exclude_fields");
//    if (null != excludeFieldsStr) {
//      Set<String> fields = StringUtils.splitCsv(excludeFieldsStr);
//      if (null != fields && !fields.isEmpty()) {
//        sp.setExcludeFields(fields);
//      }
//    }
//
//    // output_lowercase_field_names
//    if (cmd.hasOption("lowercase_names")) {
//      sp.setOutputLowercase(true);
//    }
//
//    String queryStr = cmd.getOptionValue("query");
//    if (null != queryStr) {
//      sp.setQuery(queryStr);
//    }
//
//    // Do it!
//    long start = System.currentTimeMillis();
//    long submittedCount = sp.processAll();
//    long end = System.currentTimeMillis();
//    long diff = end - start;
//    System.out.println( "Submitted " + submittedCount + " in " + diff + " ms" );
//    System.out.println( "Reminder: COMMIT_DELAY = " + COMMIT_DELAY + " ms" );

  }

}
