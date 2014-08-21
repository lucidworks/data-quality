package com.lucidworks.dq.diff;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.data.EmptyFieldStats;
import com.lucidworks.dq.schema.Schema;
import com.lucidworks.dq.schema.SchemaFromRest;
import com.lucidworks.dq.schema.SchemaFromXml;
import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class DiffEmptyFieldStats /*implements HasDescription*/ {
  static String HELP_WHAT_IS_IT = "Compare fields that aren't fully populated between two cores/collections.";
  static String HELP_USAGE = "DiffEmptyFieldStats";
  // final static Logger log = LoggerFactory.getLogger( TermStats.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }
  
  static Options options;

  public static String generateReport( EmptyFieldStats fieldStatsA, EmptyFieldStats fieldStatsB, String labelA, String labelB ) throws Exception {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    out.println( "========== Differences Report ==========" );
    out.println( "Schema A = " + labelA );
    out.println( "Schema B = " + labelB );

    out.println();
    addSimpleStatToReport( out, "A: Total Active Docs", fieldStatsA.getTotalDocCount() );
    addSimpleStatToReport( out, "B: Total Active Docs", fieldStatsB.getTotalDocCount() );

    out.println();
    Set<String> fieldsA = fieldStatsA.getAllFieldNames();
    Set<String> fieldsB = fieldStatsB.getAllFieldNames();
    addSetComparisonToReport( out, fieldsA, fieldsB, "All Fields" );

    out.println();
    addAllFieldStatsToReport( out, fieldStatsA, fieldStatsB );


//  // Simple Values
//  // -------------
//  // Name
//  String nameA = schemaA.getSchemaName();
//  String nameB = schemaB.getSchemaName();
//  addStringComparisionToReport( out, nameA, nameB, "Schema Name" );
//  // Version
//  float versA = schemaA.getSchemaVersion();
//  float versB = schemaB.getSchemaVersion();
//  out.print( "Schema Version: " );
//  if ( versA == versB ) {
//    out.println( "Both = '" + versA + "'" );
//  }
//  else {
//    out.println( "\tA = '" + versA + "'" );
//    out.println( "\tB = '" + versB + "'" );
//  }

//  // Complex Values
//  // --------------
//  // Fields
//  Set<String> fieldsA = schemaA.getAllSchemaFieldNames();
//  Set<String> fieldsB = schemaB.getAllSchemaFieldNames();
//  addSetComparisonToReport( out, fieldsA, fieldsB, "Fields" );
//  // Dynamic Field Patterns
//  // TODO: Verify that order is being preserved through the entire process
//  Set<String> patternsA = schemaA.getAllDynamicFieldPatterns();
//  Set<String> patternsB = schemaB.getAllDynamicFieldPatterns();
//  addSetComparisonToReport( out, patternsA, patternsB, "Dynamic-Field Patterns", true );

    String outStr = sw.toString();
    return outStr;
  }

  static void addAllFieldStatsToReport( PrintWriter out, EmptyFieldStats fieldStatsA, EmptyFieldStats fieldStatsB ) {
    Set<String> fieldsA = fieldStatsA.getAllFieldNames();
    Set<String> fieldsB = fieldStatsB.getAllFieldNames();
    Set<String> allFields = SetUtils.union_nonDestructive( fieldsA, fieldsB );

    // Fully Populated
    Set<String> fullFieldsA = fieldStatsA.getFullyPopulatedIndexedFields();
    Set<String> fullFieldsB = fieldStatsB.getFullyPopulatedIndexedFields();
    // Subset
    Set<String> fullFieldsBoth = SetUtils.intersection_nonDestructive( fullFieldsA, fullFieldsB );

    // Empty
    Set<String> emptyFieldsA = fieldStatsA.getFieldsWithNoIndexedValues();
    Set<String> emptyFieldsB = fieldStatsB.getFieldsWithNoIndexedValues();
    // Subset
    Set<String> emptyFieldsBoth = SetUtils.intersection_nonDestructive( emptyFieldsA, emptyFieldsB );

    // All Other Fields
    // We can only summarize the subsets of completely full and completely empty fields in both collections
    // All other fields need to be listed in the detailed report
    Set<String> detailFields = new LinkedHashSet<>();
    detailFields.addAll( allFields );
    detailFields.removeAll( fullFieldsBoth );
    detailFields.removeAll( emptyFieldsBoth );

    out.println( "Populated at 100% in Both A and B: " + fullFieldsBoth );
    out.println();
    out.println( "No Indexed Values / 0% in Both A and B: " + emptyFieldsBoth );
    out.println();

    out.println( "Partially Populated Fields and Percentages, A / B:" );
    for ( String name : detailFields ) {
      Long countA = null;
      if ( fieldStatsA.getIndexedValueCounts().containsKey(name) ) {
        countA = fieldStatsA.getIndexedValueCounts().get(name);
      }
      Double percentA = null;
      if ( fieldStatsA.getIndexedValuePercentages().containsKey(name) ) {
        percentA = fieldStatsA.getIndexedValuePercentages().get( name );
      }
      Long countB = null;
      if ( fieldStatsB.getIndexedValueCounts().containsKey(name) ) {
        countB = fieldStatsB.getIndexedValueCounts().get(name);
      }
      Double percentB = null;
      if ( fieldStatsB.getIndexedValuePercentages().containsKey(name) ) {
        percentB = fieldStatsB.getIndexedValuePercentages().get( name );
      }
      addStatsPairAndPercentToReport( out, name, countA, countB, percentA, percentB, "\t" );
    }
  }

  static void addSimpleStatToReport( PrintWriter out, String label, long stat ) {
    String statStr = NumberFormat.getNumberInstance().format( stat );
    out.println( "" + label + ": " + statStr );
  }

  static void addStringComparisionToReport( PrintWriter out, String thingA, String thingB, String attrLabel ) {
    out.print( attrLabel + ":" );
    if ( null!=thingA && null!=thingB && thingA.equals(thingB) ) {
      out.println( " Both = '" + thingA + "'" );
    }
    else {
      out.println();
      out.println( "\tA = '" + thingA + "'" );
      out.println( "\tB = '" + thingB + "'" );
    }  
  }

  static void addStatsPairAndPercentToReport( PrintWriter out, String label, Long statA, Long statB, Double percA, Double percB, String optIndent ) {
    if ( null!=optIndent ) {
      out.print( optIndent );
    }
    String statStrA = null!=statA ? NumberFormat.getNumberInstance().format( statA ) : "(not in A)";
    String statStrB = null!=statB ? NumberFormat.getNumberInstance().format( statB ) : "(not in B)";
    String percStrA = null!=percA ? " (" + MessageFormat.format( "{0,number,#.##%}" + ")", percA ) : "";
    String percStrB = null!=percB ? " (" + MessageFormat.format( "{0,number,#.##%}" + ")", percB ) : "";
    out.println( "" + label + ": " + statStrA + percStrA + " / " + statStrB + percStrB );
  }


  static void addSetComparisonToReport( PrintWriter out, Set<String> setA, Set<String> setB, String attrLabel ) {
    addSetComparisonToReport( out, setA, setB, attrLabel, false );
  }
  static void addSetComparisonToReport( PrintWriter out, Set<String> setA, Set<String> setB, String attrLabel, boolean checkOrder ) {
    Set<String> inBoth = SetUtils.intersection_nonDestructive( setA, setB );
    Set<String> inAOnly = SetUtils.inAOnly_nonDestructive( setA, setB );
    Set<String> inBOnly = SetUtils.inBOnly_nonDestructive( setA, setB );
    out.println();
    out.print( attrLabel + ":" );
    if ( inBoth.isEmpty() && inAOnly.isEmpty() && inBOnly.isEmpty() ) {
      out.println( " None!" );
    }
    else {
      out.println();
      if ( ! inBoth.isEmpty() ) {
        if ( ! checkOrder ) {
          out.println( "\tIn both = '" + inBoth + "'" );
        }
        else {
          // Note: Sets don't normally perserve order but I've been careful
          // to use LinkedHashSet and LinkedHashMap, which DO
          Set<String> commonA = SetUtils.intersection_nonDestructive( setA, setB );
          Set<String> commonB = SetUtils.intersection_nonDestructive( setB, setA );
          boolean inSameOrder = SetUtils.sameAndInSameOrder( commonA, commonB );
          if ( inSameOrder ) {
            out.println( "\tIn both and SAME relative order = '" + inBoth + "'" );
          }
          else {
            out.println( "\tIn both but DIFFERENT relative order:" );
            out.println( "\t\tCommon, order in A = '" + commonA + "'" );
            out.println( "\t\tCommon, order in B = '" + commonB + "'" );
          }
        }
      }
      if ( ! inAOnly.isEmpty() ) {
        out.println( "\tA only = '" + inAOnly + "'" );
      }
      if ( ! inBOnly.isEmpty() ) {
        out.println( "\tB only = '" + inBOnly + "'" );
      }
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
    options.addOption( "u", "url_a", true, "URL for first Solr, OR set host, port and possibly collection" );
    options.addOption( "h", "host_a", true, "IP address for first Solr, default=localhost" );
    options.addOption( "p", "port_a", true, "Port for first Solr, default=8983" );
    options.addOption( "c", "collection_a", true, "Collection/Core for first Solr, Eg: collection1" );
    options.addOption( "U", "url_b", true, "URL for second Solr, OR set host, port and possibly collection" );
    options.addOption( "H", "host_b", true, "IP address for second Solr, default=localhost" );
    options.addOption( "P", "port_b", true, "Port for second Solr, default=8983" );
    options.addOption( "C", "collection_b", true, "Collection/Core for second Solr, Eg: collection1" );

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

    String fullUrlA = cmd.getOptionValue( "url_a" );
    String hostA = cmd.getOptionValue( "host_a" );
    String portA = cmd.getOptionValue( "port_a" );
    String collA = cmd.getOptionValue( "collection_a" );
    if ( null==fullUrlA && null==hostA ) {
      helpAndExit( "Must specifify at least url or host for first Solr", 3 );
    }
    if ( null!=fullUrlA && null!=hostA ) {
      helpAndExit( "Must not specifify both url and host for first Solr", 4 );
    }

    String fullUrlB = cmd.getOptionValue( "url_b" );
    String hostB = cmd.getOptionValue( "host_b" );
    String portB = cmd.getOptionValue( "port_b" );
    String collB = cmd.getOptionValue( "collection_b" );
    if ( null==fullUrlB && null==hostB ) {
      helpAndExit( "Must specifify at least url or host for second Solr", 3 );
    }
    if ( null!=fullUrlB && null!=hostB ) {
      helpAndExit( "Must not specifify both url and host for second Solr", 4 );
    }

    // Init
    // HttpSolrServer solrA = SolrUtils.getServer( HOST1, PORT1, COLL1 );
    HttpSolrServer solrA;
    if ( null!=fullUrlA ) {
      solrA = SolrUtils.getServer( fullUrlA );
    }
    else {
      // Utils handle null values
      solrA = SolrUtils.getServer( hostA, portA, collA );    
    }
    System.out.println( "First Solr / Solr A = " + solrA.getBaseURL() );
    // HttpSolrServer solrB = SolrUtils.getServer( HOST2, PORT2, COLL2 );
    HttpSolrServer solrB;
    if ( null!=fullUrlB ) {
      solrB = SolrUtils.getServer( fullUrlB );
    }
    else {
      // Utils handle null values
      solrB = SolrUtils.getServer( hostB, portB, collB );    
    }
    System.out.println( "Second Solr / Solr B = " + solrB.getBaseURL() );

    String labelA = solrA.getBaseURL();
    EmptyFieldStats fieldsStatsA = new EmptyFieldStats( solrA );
    String reportA = fieldsStatsA.generateReport( labelA );

    String labelB = solrB.getBaseURL();
    EmptyFieldStats fieldsStatsB = new EmptyFieldStats( solrB );
    String reportB = fieldsStatsB.generateReport( labelB );

    System.out.println( "========== Individual Reports ==========" );
    System.out.println();
    System.out.println( "---------- A: " + labelA + " ----------" );
    System.out.println( reportA );
    System.out.println( "---------- B: " + labelB + " ----------" );
    System.out.println( reportB );

    String report = generateReport( fieldsStatsA, fieldsStatsB, labelA, labelB );
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