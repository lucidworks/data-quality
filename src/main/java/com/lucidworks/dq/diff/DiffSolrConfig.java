package com.lucidworks.dq.diff;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;



//import org.apache.commons.lang.StringUtils;
import com.lucidworks.dq.util.StringUtils;
import com.lucidworks.dq.schema.Schema;
import com.lucidworks.dq.schema.SchemaFromRest;
import com.lucidworks.dq.schema.SchemaFromXml;
import com.lucidworks.dq.schema.SolrConfig;
import com.lucidworks.dq.schema.SolrConfigFromXml;
import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DiffSolrConfig /*implements HasDescription*/ {
  static String HELP_WHAT_IS_IT = "WARNING: INCOMPLETE! Compare solrconfig between two cores/collections.";
  static String HELP_USAGE = "DiffConfig";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  static String MODE_FULL = "full_report";
  static String MODE_DIFF_ALL = "diff_all";
  static String MODE_DIFF_MIN = "diff_min";
  static String DEFAULT_MODE = MODE_FULL;
  static Set<String> VALID_MODES = new LinkedHashSet<String>() {{
    add( MODE_FULL );
    add( MODE_DIFF_ALL );
    add( MODE_DIFF_MIN );
  }};

  
  static Options options;

  public static String generateReport( SolrConfig configA, SolrConfig configB, String labelA, String labelB, String mode ) throws Exception {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    if ( mode.equals(MODE_DIFF_ALL) ) {
      out.println( "========== Differences Report ==========" );
      out.println( "Config A = " + labelA );
      out.println( "Config B = " + labelB );
  
      out.println();
    }

    // Simple Values
    // -------------

    // Version
    String versA = configA.getLuceneMatchVersion();
    String versB = configB.getLuceneMatchVersion();
    addStringComparisionToReport( out, versA, versB, "Lucene Version", mode );

    // Abort on Config Error
    String abortA = configA.getAbortOnConfigurationError();
    String abortB = configB.getAbortOnConfigurationError();
    addStringComparisionToReport( out, abortA, abortB, "Abort on config error", mode );

    // Complex Values
    // --------------

    // Request Handlers
    Collection<String> handlersA = configA.getRequestHandlers();
    Collection<String> handlersB = configB.getRequestHandlers();
    addSetComparisonToReport( out, handlersA, handlersB, "Handlers and Classes", mode );

    String outStr = sw.toString();
    return outStr;
  }

  static void addStringComparisionToReport( PrintWriter out, String thingA, String thingB, String attrLabel, String mode ) {
    if ( null!=thingA && null!=thingB && thingA.equals(thingB) ) {
      if ( mode.equals(MODE_DIFF_ALL) ) {
        out.print( attrLabel + ":" );
        out.println( " Both = '" + thingA + "'" );
      }
    }
    else {
      out.println( attrLabel + ":" );
      out.println( "\tA = '" + thingA + "'" );
      out.println( "\tB = '" + thingB + "'" );
    }  
  }
  static void addSetComparisonToReport( PrintWriter out, Collection<String> setA, Collection<String> setB, String attrLabel, String mode ) {
    addSetComparisonToReport( out, setA, setB, attrLabel, mode, false );
  }
  static void addSetComparisonToReport( PrintWriter out, Collection<String> setA, Collection<String> setB, String attrLabel, String mode, boolean checkOrder ) {
    Collection<String> inBoth = SetUtils.intersection_nonDestructive( setA, setB );
    Collection<String> inAOnly = SetUtils.inAOnly_nonDestructive( setA, setB );
    Collection<String> inBOnly = SetUtils.inBOnly_nonDestructive( setA, setB );
    boolean haveShownHeader = false;
    // out.println();
    // out.print( attrLabel + ":" );
    if ( inBoth.isEmpty() && inAOnly.isEmpty() && inBOnly.isEmpty() ) {
      if ( mode.equals(MODE_DIFF_ALL) ) {
        out.println();
        out.println( attrLabel + ":" );
        haveShownHeader = true;
        // out.println( " None!" );
        addNestedListToReport( out, "Both", null );
      }
    }
    else {
      // out.println();

      if ( ! inBoth.isEmpty() ) {
        //out.println();
        if ( ! checkOrder ) {
          if ( mode.equals(MODE_DIFF_ALL) ) {
            out.println();
            out.println( attrLabel + ":" );
            haveShownHeader = true;
            // out.println( "\tIn both = '" + inBoth + "'" );
            addNestedListToReport( out, "In Both", inBoth );
          }
        }
        else {
          // Note: Sets don't normally perserve order but I've been careful
          // to use LinkedHashSet and LinkedHashMap, which DO
          Collection<String> commonA = SetUtils.intersection_nonDestructive( setA, setB );
          Collection<String> commonB = SetUtils.intersection_nonDestructive( setB, setA );
          boolean inSameOrder = SetUtils.sameAndInSameOrder( commonA, commonB );
          if ( inSameOrder ) {
            if ( mode.equals(MODE_DIFF_ALL) ) {
              out.println();
              out.println( attrLabel + ":" );
              haveShownHeader = true;
              //out.println( "\tIn both and SAME relative order = '" + inBoth + "'" );
              addNestedListToReport( out, "In both and SAME relative order", inBoth );
            }
          }
          else {
            // Always print differences, regardless of mode
            // In this case call told us that ordering DOES matter and it was found to be different
            out.println();
            out.println( attrLabel + ":" );
            haveShownHeader = true;
            out.println( "\tIn both but DIFFERENT relative order:" );
            //out.println( "\t\tCommon, order in A = '" + commonA + "'" );
            addNestedListToReport( out, "Common, order in A", commonA, 2 );
            //out.println( "\t\tCommon, order in B = '" + commonB + "'" );
            addNestedListToReport( out, "Common, order in B", commonB, 2 );
          }
        }
      }

      // Always print differences, regardless of mode
      if ( ! inAOnly.isEmpty() ) {
        if ( ! haveShownHeader ) {
          out.println();
          out.println( attrLabel + ":" );
          haveShownHeader = true;
        }
        //out.println( "\tA only = '" + inAOnly + "'" );
        addNestedListToReport( out, "A only", inAOnly );
      }

      // Always print differences, regardless of mode
      if ( ! inBOnly.isEmpty() ) {
        if ( ! haveShownHeader ) {
          out.println();
          out.println( attrLabel + ":" );
          haveShownHeader = true;
        }
        //out.println( "\tB only = '" + inBOnly + "'" );
        addNestedListToReport( out, "B only", inBOnly );
      }


    }

  }
  static void addNestedListToReport( PrintWriter out, String subLabel, Collection<String> optList ) {
    addNestedListToReport( out, subLabel, optList, 1 );
  }
  static void addNestedListToReport( PrintWriter out, String subLabel, Collection<String> optList, int numTabs ) {
    //addNestedListToReport_Compact( out, subLabel, optList, numTabs );
    addNestedListToReport_Newlines( out, subLabel, optList, numTabs );
  }
  static void addNestedListToReport_Compact( PrintWriter out, String subLabel, Collection<String> optList, int numTabs ) {
    //String indent = StringUtils.repeat( "foo", 5 );
    String indent = StringUtils.repeatString( "\t", numTabs );
    // A only, B only, Both
    out.print( indent + subLabel + " = " );
    if ( null!=optList && ! optList.isEmpty() ) {
      out.println( optList );
    }
    else {
      out.println( "None!" );
    }    
  }
  static void addNestedListToReport_Newlines( PrintWriter out, String subLabel, Collection<String> optList, int numTabs ) {
    String indent1 = StringUtils.repeatString( "\t", numTabs );
    String indent2 = StringUtils.repeatString( "\t", numTabs+1 );
    // A only, B only, Both
    out.println( indent1 + subLabel + ":" );
    if ( null!=optList && ! optList.isEmpty() ) {
      for ( String item : optList ) {
        out.println( indent2 + item );
      }
    }
    else {
      out.println( indent2 + "None!" );
    }    
  }

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
    formatter.printHelp( HELP_USAGE, options, true );
    System.exit( errorCode );
  }

  public static void main( String[] argv ) throws Exception {
    options = new Options();

    // TODO: can also get config from 3x via URL, but very different format
    //options.addOption( "u", "url_a", true, "URL for first Solr, OR set host, port and possibly collection" );
    //options.addOption( "h", "host_a", true, "IP address for first Solr, default=localhost" );
    //options.addOption( "p", "port_a", true, "Port for first Solr, default=8983" );
    //options.addOption( "c", "collection_a", true, "Collection/Core for first Solr, Eg: collection1" );
    options.addOption( "f", "file_a", true, "Path to solrconfig.xml file for first Solr" );
    options.addOption( "d", "default_a", false, "Use Solr's default solrconfig.xml for the first one." );

    //options.addOption( "U", "url_b", true, "URL for second Solr, OR set host, port and possibly collection" );
    //options.addOption( "H", "host_b", true, "IP address for second Solr, default=localhost" );
    //options.addOption( "P", "port_b", true, "Port for second Solr, default=8983" );
    //options.addOption( "C", "collection_b", true, "Collection/Core for second Solr, Eg: collection1" );
    options.addOption( "F", "file_b", true, "Path to solrconfig.xml file for second Solr" );
    options.addOption( "D", "default_b", false, "Use Solr's default solrconfig.xml for the second one." );

    options.addOption( "m", "mode", true,
        "What to output:"
            + " \"" + MODE_FULL + "\" = Full reports for all of Config A, then Config B, and then full *Diff* Report (default/verbose mode)"
            + ", \"" + MODE_DIFF_ALL + "\" = Only the full Diff Report"
            + ", \"" + MODE_DIFF_MIN + "\" = Only actual differences, items in common NOT SHOWN"
        );  

    if ( argv.length < 1 ) {
      helpAndExit();
    }
    CommandLine cmd = null;
    try {
      CommandLineParser parser = new PosixParser();
      // CommandLineParser parser = new DefaultParser();
      cmd = parser.parse( options, argv );
    }
    catch( ParseException exp ) {
      helpAndExit( "Parsing command line failed. Reason: " + exp.getMessage(), 2 );
    }
    // Already using -h for host, don't really need help, just run with no options
    //if ( cmd.hasOption("help") ) {
    //  helpAndExit();
    //}

    // Config A
    //String fullUrlA = cmd.getOptionValue( "url_a" );
    //String hostA = cmd.getOptionValue( "host_a" );
    //String portA = cmd.getOptionValue( "port_a" );
    //String collA = cmd.getOptionValue( "collection_a" );
    String fileA = cmd.getOptionValue( "file_a" );
    boolean useDefaultA = false;
    if( cmd.hasOption("default_a") ) {
      useDefaultA = true;
    }
    // Option Count
    int countA = 0;
    //countA = null!=fullUrlA ? 1 : 0;
    //countA += null!=hostA ? 1 : 0;
    countA += null!=fileA ? 1 : 0;
    countA += useDefaultA ? 1 : 0;
    if ( countA < 1 ) {
      //helpAndExit( "Must specifify one of: url, host, file or default, for first Solr", 3 );
      helpAndExit( "Must specifify one of: file or default, for first Solr", 3 );
    }
    if ( countA > 1 ) {
      //helpAndExit( "Must not specifify more than one of: url, host, file or default, for first Solr", 4 );
      helpAndExit( "Must not specifify more than one of: file or default, for first Solr", 4 );
    }
    // Schema schemaA = new SchemaFromXml();
    // String labelA = "Default Solr 4.6.1 Schema";
    SolrConfig configA = null;
    String labelA = null;
    //    if ( null!=fullUrlA ) {
    //      HttpSolrServer solrA = SolrUtils.getServer( fullUrlA );
    //      labelA = solrA.getBaseURL();
    //      configA = new SolrConfigFromRest( solrA );
    //    }
    //    else if ( null!=hostA ) {
    //      // util handles null values
    //      HttpSolrServer solrA = SolrUtils.getServer( hostA, portA, collA );
    //      labelA = solrA.getBaseURL();
    //      configA = new SolrConfigFromRest( solrA );
    //    }
    //    else
    if ( null!=fileA ) {
      labelA = "XML File: " + fileA;
      configA = new SolrConfigFromXml( new File(fileA) );
    }
    else if ( useDefaultA ) {
      labelA = "Default Solr 4x Config";
      configA = new SolrConfigFromXml();
    }

    // Config B
    //    String fullUrlB = cmd.getOptionValue( "url_b" );
    //    String hostB = cmd.getOptionValue( "host_b" );
    //    String portB = cmd.getOptionValue( "port_b" );
    //    String collB = cmd.getOptionValue( "collection_b" );
    String fileB = cmd.getOptionValue( "file_b" );
    boolean useDefaultB = false;
    if( cmd.hasOption("default_b") ) {
      useDefaultB = true;
    }
    // Option Count
    int countB = 0;
    //countB = null!=fullUrlB ? 1 : 0;
    //countB += null!=hostB ? 1 : 0;
    countB += null!=fileB ? 1 : 0;
    countB += useDefaultB ? 1 : 0;
    if ( countB < 1 ) {
      //helpAndExit( "Must specifify one of: url, host, file or default, for second Solr", 3 );
      helpAndExit( "Must specifify one of: file or default, for second Solr", 3 );
    }
    if ( countB > 1 ) {
      //helpAndExit( "Must not specifify more than one of: url, host, file or default, for second Solr", 4 );
      helpAndExit( "Must not specifify more than one of: file or default, for second Solr", 4 );
    }
    // Schema schemaB = new SchemaFromRest( HOST0, PORT0, COLL0 );
    // String labelB = "Apollo demo plus local changes";
    SolrConfig configB = null;
    String labelB = null;
    //    if ( null!=fullUrlB ) {
    //      HttpSolrServer solrB = SolrUtils.getServer( fullUrlB );
    //      labelB = solrB.getBaseURL();
    //      schemaB = new SchemaFromRest( solrB );
    //    }
    //    else if ( null!=hostB ) {
    //      // util handles null values
    //      HttpSolrServer solrB = SolrUtils.getServer( hostB, portB, collB );
    //      labelB = solrB.getBaseURL();
    //      schemaB = new SchemaFromRest( solrB );
    //    }
    //    else
    if ( null!=fileB ) {
      labelB = "XML File: " + fileB;
      configB = new SolrConfigFromXml( new File(fileB) );
    }
    else if ( useDefaultB ) {
      labelB = "Default Solr 4x Config";
      configB = new SolrConfigFromXml();
    }

    // VALID_MODES
    String mode = cmd.getOptionValue( "mode" );
    if ( null!=mode ) {
      mode = mode.toLowerCase().trim();
      if ( ! VALID_MODES.contains(mode) ) {
        helpAndExit( "Invalid mode, must be one of: " + VALID_MODES, 5 );       
      }
    }
    else {
      mode = DEFAULT_MODE;
    }

    String reportA = configA.generateReport();
    String reportB = configB.generateReport();

    if ( mode.equals(MODE_FULL) ) {
      System.out.println( "========== Individual Reports ==========" );
      System.out.println();
      System.out.println( "---------- A: " + labelA + " ----------" );
      System.out.println( reportA );
      System.out.println( "---------- B: " + labelB + " ----------" );
      System.out.println( reportB );
    }

    String report = generateReport( configA, configB, labelA, labelB, mode );
    System.out.println( report );
  }


  static String HOST0 = "localhost";
  static String PORT0 = "8983";
  static String COLL0 = "demo_shard1_replica1";
  static String URL0 = "http://" + HOST0 + ":" + PORT0 + "/solr/" + COLL0;
  // + "/select?q=*:*&rows=" + ROWS + "&fl=id&wt=json&indent=on"

  static String HOST1 = "localhost";
  static String PORT1 = "8984"; // "8983";
  static String COLL1 = "collection1";
  static String URL1 = "http://" + HOST1 + ":" + PORT1 + "/solr/" + COLL1;

  static String HOST2 = "localhost";
  static String PORT2 = "8985"; // "8983";
  static String COLL2 = "collection1";
  static String URL2 = "http://" + HOST1 + ":" + PORT2 + "/solr/" + COLL2;

}