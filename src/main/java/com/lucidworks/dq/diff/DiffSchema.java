package com.lucidworks.dq.diff;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.schema.Schema;
import com.lucidworks.dq.schema.SchemaFromRest;
import com.lucidworks.dq.schema.SchemaFromXml;
import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DiffSchema /*implements HasDescription*/ {
  static String HELP_WHAT_IS_IT = "Compare schemas between two cores/collections.";
  static String HELP_USAGE = "DiffSchema";
  // final static Logger log = LoggerFactory.getLogger( TermStats.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }
  
  static Options options;

  public static String generateReport( Schema schemaA, Schema schemaB, String labelA, String labelB ) throws Exception {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    out.println( "========== Differences Report ==========" );
    out.println( "Schema A = " + labelA );
    out.println( "Schema B = " + labelB );

    out.println();

    // Simple Values
    // -------------

    // Name
    String nameA = schemaA.getSchemaName();
    String nameB = schemaB.getSchemaName();
    addStringComparisionToReport(out, nameA, nameB, "Schema Name");

    // Version
    float versA = schemaA.getSchemaVersion();
    float versB = schemaB.getSchemaVersion();
    out.print("Schema Version: ");
    if (versA == versB) {
      out.println("Both = '" + versA + "'");
    } else {
      out.println("\tA = '" + versA + "'");
      out.println("\tB = '" + versB + "'");
    }

    // Key Field
    String keyA = schemaA.getUniqueKeyFieldName();
    String keyB = schemaB.getUniqueKeyFieldName();
    addStringComparisionToReport( out, keyA, keyB, "Key Field" );

    // Default Operator
    String defOpA = schemaA.getDefaultOperator();
    String defOpB = schemaB.getDefaultOperator();
    addStringComparisionToReport( out, defOpA, defOpB, "Default Operator" );

    // Similarity
    String simA = schemaA.getSimilarityModelClassName();
    String simB = schemaB.getSimilarityModelClassName();
    addStringComparisionToReport( out, simA, simB, "Similarity Class Name" );

    // Complex Values
    // --------------

    // Fields
    Set<String> fieldsA = schemaA.getAllSchemaFieldNames();
    Set<String> fieldsB = schemaB.getAllSchemaFieldNames();
    addSetComparisonToReport( out, fieldsA, fieldsB, "Fields" );

    // Dynamic Field Patterns
    // TODO: Verify that order is being preserved through the entire process
    Set<String> patternsA = schemaA.getAllDynamicFieldPatterns();
    Set<String> patternsB = schemaB.getAllDynamicFieldPatterns();
    addSetComparisonToReport( out, patternsA, patternsB, "Dynamic-Field Patterns", true );

    // Types
    Set<String> typeNamesA = schemaA.getAllFieldTypeNames();
    Set<String> typeNamesB = schemaB.getAllFieldTypeNames();
    addSetComparisonToReport( out, typeNamesA, typeNamesB, "Types" );

    // Types to Fields
    Map<String, Set<String>> typeToFieldsA = schemaA.getAllDeclaredAndDynamicFieldsByType();
    Map<String, Set<String>> typeToFieldsB = schemaB.getAllDeclaredAndDynamicFieldsByType();
    addMapOfSetsComparisonToReport( out, typeToFieldsA, typeToFieldsB, "Type -> Fields: (declared and dynamic patterns)" );

    // TODO: For common fields, compare types for each field (need lookuip method)
    // TODO: For common fileds, compare other attrs (not yet visibile here)

    // Copy Sources
    Set<String> sourcesA = schemaA.getAllCopyFieldSourceNames();
    Set<String> sourcesB = schemaB.getAllCopyFieldSourceNames();
    addSetComparisonToReport( out, sourcesA, sourcesB, "Copy-Field Sources" );

    // Copy Destinations
    Set<String> destsA = schemaA.getAllCopyFieldDestinationNames();
    Set<String> destsB = schemaB.getAllCopyFieldDestinationNames();
    addSetComparisonToReport( out, destsA, destsB, "Copy-Field Destinations" );

    // TODO: For common copy field sources, compare destinations

    String outStr = sw.toString();
    return outStr;
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
      // finish previous line
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

  static void addMapOfSetsComparisonToReport( PrintWriter out, Map<String, Set<String>> mapA, Map<String, Set<String>> mapB, String attrLabel ) {
    Set<String> keysInBoth = SetUtils.intersection_nonDestructive( mapA.keySet(), mapB.keySet() );
    Set<String> keysInAOnly = SetUtils.inAOnly_nonDestructive( mapA.keySet(), mapB.keySet() );
    Set<String> keysInBOnly = SetUtils.inBOnly_nonDestructive( mapA.keySet(), mapB.keySet() );
    out.println();
    out.print( attrLabel + ":" );
    if ( keysInBoth.isEmpty() && keysInAOnly.isEmpty() && keysInBOnly.isEmpty() ) {
      out.println( " None!" );
    }
    else {
      // finish previous line
      out.println();
      
      if ( ! keysInBoth.isEmpty() ) {
        out.println( "Types in both A and B:" );
        out.println( "\t(" + keysInBoth.size() + " A/B common types)" );
        for ( String type : keysInBoth ) {
          out.println( "\tType: " + type );

          Set<String> typeAFields = mapA.get( type );
          Set<String> typeBFields = mapB.get( type );
          Set<String> fieldsInBoth = SetUtils.intersection_nonDestructive( typeAFields, typeBFields );
          Set<String> fieldsInAOnly = SetUtils.inAOnly_nonDestructive( typeAFields, typeBFields );
          Set<String> fieldsInBOnly = SetUtils.inBOnly_nonDestructive( typeAFields, typeBFields );

          // A and B
          if ( ! fieldsInBoth.isEmpty() ) {
            out.println( "\t\tFields of type \"" + type + "\" in both A and B:" );
            out.println( "\t\t\t(" + fieldsInBoth.size() + " fields)" );
            for ( String field : fieldsInBoth ) {
              out.println( "\t\t\t" + field );            
            }
          }
          // A-only fields
          if ( ! fieldsInAOnly.isEmpty() ) {
            out.println( "\t\tFields of type \"" + type + "\" only in A:" );
            out.println( "\t\t\t(" + fieldsInAOnly.size() + " fields)" );
            for ( String field : fieldsInAOnly ) {
              out.println( "\t\t\t" + field );            
            }
          }
          // B-only fields
          if ( ! fieldsInBOnly.isEmpty() ) {
            out.println( "\t\tFields of type \"" + type + "\" only in B:" );
            out.println( "\t\t\t(" + fieldsInBOnly.size() + " fields)" );
            for ( String field : fieldsInBOnly ) {
              out.println( "\t\t\t" + field );            
            }
          }
        }        
      }
      // A-only Types
      if ( ! keysInAOnly.isEmpty() ) {
        out.println( "A only:" );
        // + inAOnly + "'" );
        out.println( "\t(" + keysInAOnly.size() + " A-only types)" );
        for ( String type : keysInAOnly ) {
          out.println( "\tType: " + type );

          // out.println( "\t\t" + type + ":" );
          Set<String> typeFields = mapA.get( type );
          out.println( "\t\t(" + typeFields.size() + " fields of this type only in A)" );
          for ( String field : typeFields ) {
            out.println( "\t\t" + field );        
          }
        }
      }
      // B-only Types
      if ( ! keysInBOnly.isEmpty() ) {
        out.println( "B only:" );
        out.println( "\t(" + keysInBOnly.size() + " B-only types)" );
        for ( String type : keysInBOnly ) {
          out.println( "\tType: " + type );
          //out.println( "\t\t" + type + ":" );
          Set<String> typeFields = mapB.get( type );
          out.println( "\t\t(" + typeFields.size() + " fields of this type only in B)" );
          for ( String field : typeFields ) {
            out.println( "\t\t" + field );        
          }
        }
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
    options.addOption( "f", "file_a", true, "Path to schema.xml file for first Solr" );
    options.addOption( "d", "default_a", false, "Use Solr's default schema.xml for the first one." );

    options.addOption( "U", "url_b", true, "URL for second Solr, OR set host, port and possibly collection" );
    options.addOption( "H", "host_b", true, "IP address for second Solr, default=localhost" );
    options.addOption( "P", "port_b", true, "Port for second Solr, default=8983" );
    options.addOption( "C", "collection_b", true, "Collection/Core for second Solr, Eg: collection1" );
    options.addOption( "F", "file_b", true, "Path to schema.xml file for second Solr" );
    options.addOption( "D", "default_b", false, "Use Solr's default schema.xml for the second one." );

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

    // Schema A
    String fullUrlA = cmd.getOptionValue( "url_a" );
    String hostA = cmd.getOptionValue( "host_a" );
    String portA = cmd.getOptionValue( "port_a" );
    String collA = cmd.getOptionValue( "collection_a" );
    String fileA = cmd.getOptionValue( "file_a" );
    boolean useDefaultA = false;
    if( cmd.hasOption("default_a") ) {
      useDefaultA = true;
    }
    // Option Count
    int countA = null!=fullUrlA ? 1 : 0;
    countA += null!=hostA ? 1 : 0;
    countA += null!=fileA ? 1 : 0;
    countA += useDefaultA ? 1 : 0;
    if ( countA < 1 ) {
      helpAndExit( "Must specifify one of: url, host, file or default, for first Solr", 3 );
    }
    if ( countA > 1 ) {
      helpAndExit( "Must not specifify more than one of: url, host, file or default, for first Solr", 4 );
    }
    // Schema schemaA = new SchemaFromXml();
    // String labelA = "Default Solr 4.6.1 Schema";
    Schema schemaA = null;
    String labelA = null;
    if ( null!=fullUrlA ) {
      HttpSolrServer solrA = SolrUtils.getServer( fullUrlA );
      labelA = solrA.getBaseURL();
      schemaA = new SchemaFromRest( solrA );
    }
    else if ( null!=hostA ) {
      // util handles null values
      HttpSolrServer solrA = SolrUtils.getServer( hostA, portA, collA );
      labelA = solrA.getBaseURL();
      schemaA = new SchemaFromRest( solrA );
    }
    else if ( null!=fileA ) {
      labelA = "XML File: " + fileA;
      schemaA = new SchemaFromXml( new File(fileA) );
    }
    else if ( useDefaultA ) {
      labelA = "Default Solr 4.6.1 Schema";
      schemaA = new SchemaFromXml();
    }

    // Schema B
    String fullUrlB = cmd.getOptionValue( "url_b" );
    String hostB = cmd.getOptionValue( "host_b" );
    String portB = cmd.getOptionValue( "port_b" );
    String collB = cmd.getOptionValue( "collection_b" );
    String fileB = cmd.getOptionValue( "file_b" );
    boolean useDefaultB = false;
    if( cmd.hasOption("default_b") ) {
      useDefaultB = true;
    }
    // Option Count
    int countB = null!=fullUrlB ? 1 : 0;
    countB += null!=hostB ? 1 : 0;
    countB += null!=fileB ? 1 : 0;
    countB += useDefaultB ? 1 : 0;
    if ( countB < 1 ) {
      helpAndExit( "Must specifify one of: url, host, file or default, for second Solr", 3 );
    }
    if ( countB > 1 ) {
      helpAndExit( "Must not specifify more than one of: url, host, file or default, for second Solr", 4 );
    }
    // Schema schemaB = new SchemaFromRest( HOST0, PORT0, COLL0 );
    // String labelB = "Apollo demo plus local changes";
    Schema schemaB = null;
    String labelB = null;
    if ( null!=fullUrlB ) {
      HttpSolrServer solrB = SolrUtils.getServer( fullUrlB );
      labelB = solrB.getBaseURL();
      schemaB = new SchemaFromRest( solrB );
    }
    else if ( null!=hostB ) {
      // util handles null values
      HttpSolrServer solrB = SolrUtils.getServer( hostB, portB, collB );
      labelB = solrB.getBaseURL();
      schemaB = new SchemaFromRest( solrB );
    }
    else if ( null!=fileB ) {
      labelB = "XML File: " + fileB;
      schemaB = new SchemaFromXml( new File(fileB) );
    }
    else if ( useDefaultB ) {
      labelB = "Default Solr 4.6.1 Schema";
      schemaB = new SchemaFromXml();
    }

    String reportA = schemaA.generateReport();
    String reportB = schemaB.generateReport();

    System.out.println( "========== Individual Reports ==========" );
    System.out.println();
    System.out.println( "---------- A: " + labelA + " ----------" );
    System.out.println( reportA );
    System.out.println( "---------- B: " + labelB + " ----------" );
    System.out.println( reportB );

    String report = generateReport( schemaA, schemaB, labelA, labelB );
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