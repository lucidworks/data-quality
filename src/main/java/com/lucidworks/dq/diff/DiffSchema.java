package com.lucidworks.dq.diff;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;

import com.lucidworks.dq.schema.Schema;
import com.lucidworks.dq.schema.SchemaFromRest;
import com.lucidworks.dq.schema.SchemaFromXml;
import com.lucidworks.dq.util.SetUtils;

public class DiffSchema {

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
	addStringComparisionToReport( out, nameA, nameB, "Schema Name" );

    // Version
	float versA = schemaA.getSchemaVersion();
	float versB = schemaB.getSchemaVersion();
	out.print( "Schema Version: " );
	if ( versA == versB ) {
	  out.println( "Both = '" + versA + "'" );
	}
	else {
	  out.println( "\tA = '" + versA + "'" );
	  out.println( "\tB = '" + versB + "'" );
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
    Set<String> typeNamesA = schemaA.getAllSchemaFieldNames();
    Set<String> typeNamesB = schemaB.getAllSchemaFieldNames();
    addSetComparisonToReport( out, typeNamesA, typeNamesB, "Types" );
    
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

  public static void main( String[] args ) throws Exception {
	// Default 4.6.1 schema
	Schema schemaA = new SchemaFromXml();
	String labelA = "Default Solr 4.6.1 Schema";
	Schema schemaB = new SchemaFromRest( HOST0, PORT0, COLL0 );
	String labelB = "Apollo demo plus local changes";

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