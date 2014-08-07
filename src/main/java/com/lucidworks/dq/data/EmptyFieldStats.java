package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class EmptyFieldStats /*implements HasDescription*/ {
  static String HOST = "localhost";
  static int PORT = 8983;
  // static String COLL = "collection1";
  static String COLL = "demo_shard1_replica1";

  static String HELP_WHAT_IS_IT = "Look for fields that aren't fully populated.";
  static String HELP_USAGE = "EmptyFieldStats -u http://localhost:8983";
  // final static Logger log = LoggerFactory.getLogger( FieldStats.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  static Options options;
  
  // Specific command line options
  boolean includeStoredFields = false;
  boolean showIds = false;
  Set<String> targetFields = null;
  int rows = 0;
  int start = 0;

  HttpSolrServer solrServer;
  Long totalDocs;

  Set<String> allFieldNames;

  Set<String> lukeIndexedFields;
  Set<String> lukeStoredFields;
  Set<String> lukeIndexedButNotStored;
  Set<String> lukeStoredButNotIndexed;
  
  Set<String> fieldsWithIndexedValues;
  Set<String> fieldsWithNoIndexedValues;
  Set<String> fieldsWithStoredValues;
  Set<String> fieldsWithNoStoredValues;
  // Only for Non-Zero collections
  Set<String> fullCountIndexedValues;
  Set<String> lowCountIndexedValues;
  Set<String> fullCountStoredValues;
  Set<String> lowCountStoredValues;

  Map<String,Long> fieldStatsIndexedValueCounts;
  Map<String,Long> fieldStatsIndexedValueDeficits;
  // We're using Longs; also I think Java uses Dboubles internally anyway
  Map<String,Double> fieldStatsIndexedValuePercentages;
  Map<String,Long> fieldStatsStoredValueCounts;
  Map<String,Long> fieldStatsStoredValueDeficits;
  // We're using Longs; also I think Java uses Dboubles internally anyway
  Map<String,Double> fieldStatsStoredValuePercentages;

  public EmptyFieldStats( HttpSolrServer server ) throws SolrServerException {
    this( server, null, null, null, 0, 0 );
  }
  // TODO: refactor to allow options to be settable after constructor is run
  public EmptyFieldStats( HttpSolrServer server, Set<String> targetFields,
      Boolean includeStoredFields, Boolean showIds, int rows, int start ) throws SolrServerException {
    this.solrServer = server;
    if ( null!=targetFields && ! targetFields.isEmpty() ) {
      this.targetFields = targetFields;
    }
    if ( null!=includeStoredFields && includeStoredFields.booleanValue() ) {
      this.includeStoredFields = true;
    }
    if ( null!=showIds && showIds.booleanValue() ) {
      this.showIds = true;
    }
    this.rows = rows;
    this.start = start;
    resetData();
    // TODO: should defer these?  Nice sanity check...
    doAllTabulations();  
  }
  public HttpSolrServer getSolrServer() {
    return this.solrServer;
  }
  public Set<String> getTargetFields() {
    return targetFields;
  }
  public boolean getIncludeStoredFields() {
    return includeStoredFields;
  }
  public boolean getShowIds() {
    return showIds;
  }

  void resetData() throws SolrServerException {
    totalDocs = 0L;
    fieldsWithIndexedValues = new LinkedHashSet<>();
    fieldsWithNoIndexedValues = new LinkedHashSet<>();
    fullCountIndexedValues = new LinkedHashSet<>();
    lowCountIndexedValues = new LinkedHashSet<>();
    fieldsWithStoredValues = new LinkedHashSet<>();
    fieldsWithNoStoredValues = new LinkedHashSet<>();
    fullCountStoredValues = new LinkedHashSet<>();
    lowCountStoredValues = new LinkedHashSet<>();
    fieldStatsIndexedValueCounts = new LinkedHashMap<>();
    fieldStatsIndexedValueDeficits = new LinkedHashMap<>();
    fieldStatsIndexedValuePercentages = new LinkedHashMap<>();
    fieldStatsStoredValueCounts = new LinkedHashMap<>();
    fieldStatsStoredValueDeficits = new LinkedHashMap<>();
    fieldStatsStoredValuePercentages = new LinkedHashMap<>();
    allFieldNames = SolrUtils.getAllDeclaredAndActualFieldNames(solrServer);
  	lukeIndexedFields = SolrUtils.getLukeFieldsWithIndexedValues( solrServer );
  	lukeStoredFields = SolrUtils.getLukeFieldsWithStoredValues( solrServer );
  	lukeIndexedButNotStored = SetUtils.inAOnly_nonDestructive( lukeIndexedFields, lukeStoredFields );
  	lukeStoredButNotIndexed = SetUtils.inBOnly_nonDestructive( lukeIndexedFields, lukeStoredFields );
  }
  // TODO: change to public if we defer from constructor
  void doAllTabulations() throws SolrServerException {
    fetchTotalDocCountFromServer();
    // TODO: could skip some steps if 0 docs
    tabulateAllFields();
  }

  void fetchTotalDocCountFromServer() throws SolrServerException {
    totalDocs = SolrUtils.getTotalDocCount( getSolrServer() );
  }
  public long getTotalDocCount() {
    return totalDocs;
  }

  // TODO: could make these unmodifiable
  public Set<String> getAllFieldNames() {
    return allFieldNames;
  }
  public Set<String> getFieldsWithIndexedValues() {
    return fieldsWithIndexedValues;
  }
  public Set<String> getFieldsWithNoIndexedValues() {
    return fieldsWithNoIndexedValues;
  }
  public Set<String> getFieldsWithStoredValues() {
    return fieldsWithStoredValues;
  }
  public Set<String> getFieldsWithNoStoredValues() {
    return fieldsWithNoStoredValues;
  }
  // Only for Non-Empty Collection
  public Set<String> getFullyPopulatedIndexedFields() {
    return fullCountIndexedValues;
  }
  // Only for Non-Empty Collection
  public Set<String> getPartiallyPopulatedIndexedFields() {
    return lowCountIndexedValues;
  }
  public Set<String> getFullyPopulatedStoredFields() {
    return fullCountStoredValues;
  }
  public Set<String> getPartiallyPopulatedStoredFields() {
    return lowCountStoredValues;
  }

  public Map<String,Long> getIndexedValueCounts() {
	  return fieldStatsIndexedValueCounts;
  }
  public Map<String,Double> getIndexedValuePercentages() {
	  return fieldStatsIndexedValuePercentages;
  }

  void tabulateAllFields() throws SolrServerException {
    // Indexed Fields
    if ( null!=getTargetFields() ) {
      tabulateFieldsWithIndexedValues( getTargetFields() );
    }
    else {
      tabulateFieldsWithIndexedValues( allFieldNames );
    }
    // Stored Fields
    if ( getIncludeStoredFields() ) {
      if ( null!=getTargetFields() ) {
        tabulateFieldsWithStoredValues( getTargetFields() );
      }
      else {
        tabulateFieldsWithStoredValues( lukeStoredFields );
      }
    }
  }
  void tabulateFieldsWithIndexedValues( Set<String> fieldNames ) throws SolrServerException {
    for ( String field : fieldNames ) {
      long stat = SolrUtils.getDocCountForField( getSolrServer(), field );
      if ( stat > 0L ) {
        fieldsWithIndexedValues.add( field );
        if ( getTotalDocCount() > 0L ) {
          if ( stat >= getTotalDocCount() ) {
            fullCountIndexedValues.add( field );
          }
          else {
            lowCountIndexedValues.add( field );
          }
        }
      }
      else {
        fieldsWithNoIndexedValues.add( field );
      }
      fieldStatsIndexedValueCounts.put( field , stat );
      // Shouldn't be negative
      fieldStatsIndexedValueDeficits.put( field, getTotalDocCount() - stat );

      if ( getTotalDocCount() > 0L ) {
        // TODO: could just insert 0% if stat is 0, save calculation
        double percent = (double) stat / (double) getTotalDocCount();
        fieldStatsIndexedValuePercentages.put( field, percent );
      }
      else {
        // TODO: If no total docs, default to 0% ?  Or could leave empty
        fieldStatsIndexedValuePercentages.put( field, 0.0D );
      }
    }
  }
  // Tabulate number of documents that have any value for each field
  // If a field has multiple values, it still only counts for 1 here
  // 262 ms for 1,000 docs
  // 1,403 ms for 10k docs
  // 14,577 ms for 100k docs
  // 209,042 ms for 1.2M docs and *12 Gigs* of RAM
  void tabulateFieldsWithStoredValues( Set<String> fieldNames ) throws SolrServerException {
    long overallStart = System.currentTimeMillis();
    int fieldCount = 0;

    // Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getStoredValuesForFields_ByField( server, fieldNames, 100000 );
    // Map<String, Map<String, Long>> stats = SolrUtils.flattenStoredValues_ValueToTotalCount( docsByField );
    // Calling for all fields at once is a huge memory hog, can cause OOM
    // *or* is that we're trying to store all the values in memory in one structure, regardless of how we fetch them
    // Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getAllStoredValuesForFields_ByField( solrServer, fieldNames );
    Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getAllStoredValuesForFields_ByField( solrServer, fieldNames, rows, start );

    // Gives us: field name -> doc count
    Map<String,Long> stats = SolrUtils.flattenStoredValues_ToDocCount( docsByField );

    // See also second half of addAllFieldStatsToReport
    // TODO: could move this to a method
    long expectedDocCount = getTotalDocCount();
    // If we limited number of rows or skipped over some
    // then adjust our expectations
    if ( rows > 0 || start > 0 ) {
      // can't expect fewer than 0
      if ( expectedDocCount > 0 ) {
        // If skipping earlier records, than that reduces potential total
        // Eg: 15 docs in collection, but told to start at offset 5,
        // so can only see docs at offsets [5, 6, 7, 8, 9, 10, 11, 12, 13, 14] = *10*
        if ( start > 0 ) {
          expectedDocCount -= start;
          // bounds check
          expectedDocCount = expectedDocCount<0 ? 0 : expectedDocCount;
        }
        // Eg: 15 docs but rows=5 yields offsets [0, 1, 2, 3, 4], total of 5
        // Eg: 15 docs, start=5 & rows=5 yields [5, 6, 7, 8, 9], still total of 5
        // harmless if expectedDocs already 0
        if ( rows > 0 && rows < expectedDocCount ) {
          expectedDocCount = rows;
        }
      }
    }

    // Foreach field
    for ( Entry<String, Long> item : stats.entrySet() ) {
      fieldCount++;
      String fieldName = item.getKey();
      Long stat = item.getValue();
      // Record raw stat
      fieldStatsStoredValueCounts.put( fieldName, stat );
      // Any Stored Values?
      if ( stat > 0L ) {
        fieldsWithStoredValues.add( fieldName );
        // If not an empty Collection
        if ( getTotalDocCount() > 0L ) {
          //if ( stat >= getTotalDocCount() )
          if ( stat >= expectedDocCount )
          {
            fullCountStoredValues.add( fieldName );
          }
          else {
            lowCountStoredValues.add( fieldName );
          }
        }
      }
      // Else No Stored Values
      else {
        fieldsWithNoStoredValues.add( fieldName );
      }
      // Shouldn't be negative
      //fieldStatsStoredValueDeficits.put( fieldName, getTotalDocCount() - stat );
      fieldStatsStoredValueDeficits.put( fieldName, expectedDocCount - stat );
      // If not an empty Collection
      if ( getTotalDocCount() > 0L ) {
        // TODO: could just insert 0% if stat is 0, save calculation
        //double percent = (double) stat / (double) getTotalDocCount();
        double percent = (double) stat / (double) expectedDocCount;
        fieldStatsStoredValuePercentages.put( fieldName, percent );
      }
      // Else empty Collection
      else {
        // TODO: If no total docs, default to 0% ?  Or could leave empty
        fieldStatsStoredValuePercentages.put( fieldName, 0.0D );
      }
    }
    long overallStop = System.currentTimeMillis();
    long overallDiff = overallStop - overallStart;
    String diffStr = NumberFormat.getNumberInstance().format( overallDiff );
    System.out.println( "OVERALL time for ALL fields took " + diffStr + " ms for " + fieldCount + " fields" );
  }
  // 2,897 ms for 1,000 fields
  void _v0_tabulateFieldsWithStoredValues( Set<String> fieldNames ) throws SolrServerException {
    long overallStart = System.currentTimeMillis();
    int fieldCount = 0;
    for ( String field : fieldNames ) {
      fieldCount++;
      // long stat = _SolrUtils.getDocCountForField( getServer(), field );
      long start = System.currentTimeMillis();

      long stat = SolrUtils.getStoredDocCountForField( getSolrServer(), field );

      long stop = System.currentTimeMillis();
      long diff = stop - start;
      String diffStr = NumberFormat.getNumberInstance().format( diff );
      System.out.println( field + " took " + diffStr + " ms" );

      if ( stat > 0L ) {
        fieldsWithStoredValues.add( field );
        if ( getTotalDocCount() > 0L ) {
          if ( stat >= getTotalDocCount() ) {
            fullCountStoredValues.add( field );
          }
          else {
            lowCountStoredValues.add( field );
          }
        }
      }
      else {
        fieldsWithNoStoredValues.add( field );
      }
      fieldStatsStoredValueCounts.put( field , stat );
      // Shouldn't be negative
      fieldStatsStoredValueDeficits.put( field, getTotalDocCount() - stat );

      if ( getTotalDocCount() > 0L ) {
        // TODO: could just insert 0% if stat is 0, save calculation
        double percent = (double) stat / (double) getTotalDocCount();
        fieldStatsStoredValuePercentages.put( field, percent );
      }
      else {
        // TODO: If no total docs, default to 0% ?  Or could leave empty
        fieldStatsStoredValuePercentages.put( field, 0.0D );
      }
    }
    long overallStop = System.currentTimeMillis();
    long overallDiff = overallStop - overallStart;
    String diffStr = NumberFormat.getNumberInstance().format( overallDiff );
    System.out.println( "OVERALL time for ALL fields took " + diffStr + " ms for " + fieldCount + " fields" );
  }

  // TODO: could include label as a settable member field
  public String generateReport( String optLabel ) throws Exception {

    // *if* not done in constructor, nor by specific all,
    // then do it now
    // doAllTabulations();

    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    if ( null!=optLabel ) {
      out.println( "----------- " + optLabel + " -----------" );
    }
    addSimpleStatToReport( out, "Total Active Docs", getTotalDocCount() );

    out.println();
    out.println( "All Fields"
        + " (" + getAllFieldNames().size() + ")"
        + ": " + getAllFieldNames()
        );
    out.println();
    out.println( "Luke Indexed but not Stored"
        + " (" + lukeIndexedButNotStored.size() + ")"
        + ": " + lukeIndexedButNotStored
        );
    out.println( "Luke Stored but not Indexed"
        + " (" + lukeStoredButNotIndexed.size() + ")"
        + ": " + lukeStoredButNotIndexed
        );

    if ( null!=getTargetFields() ) {
      out.println();
      out.println( "LIMITING to Target Fields: " + getTargetFields() );
    }
    //    // out.println( "Fields with Indexed Values: " + getFieldsWithIndexedValues() );
    //    if ( getTotalDocCount() > 0 ) {
    //      out.println( "Fields with Fully Indexed Values: " + getFullyPopulatedIndexedFields() );
    //      out.println( "Fields with Partially Indexed Values: " + getPartiallyPopulatedIndexedFields() );
    //    }
    //    out.println( "Fields with No Indexed Values: " + getFieldsWithNoIndexedValues() );
    //    
    //    out.println( "Declared Fields with Indexed Values:" );
    out.println();
    addAllFieldStatsToReport( out );

    String outStr = sw.toString();
    return outStr;
  }
  void addAllFieldStatsToReport( PrintWriter out ) throws SolrServerException {

    // Indexed (vs. Stored)
    out.println( "Indexed at 100%"
        + " (" + getFullyPopulatedIndexedFields().size() + ")"
        + ": " + getFullyPopulatedIndexedFields()
        );
    out.println();
    out.println( "No Indexed Values / 0%"
        + " (" + getFieldsWithNoIndexedValues().size() + ")"
        + ": " + getFieldsWithNoIndexedValues()
        );
    out.println();
    // TODO: might be nice to sort by percent desc + name asc
    out.println( "Partially Indexed Fields / Percentages:" );
    out.println( "\t(" + getPartiallyPopulatedIndexedFields().size() + ")" );
    Set<String> lowFieldsIndexed = getPartiallyPopulatedIndexedFields();
    for ( String name : lowFieldsIndexed ) {
      Long count = fieldStatsIndexedValueCounts.get( name );
      Double percent = null;
      if ( fieldStatsIndexedValuePercentages.containsKey(name) ) {
        percent = fieldStatsIndexedValuePercentages.get( name );
      }
      // this shouldn't happen, parent loop shouldn't be requesting this field
      else if ( lukeIndexedFields.contains(name) ) {
        percent = 0.0D;
      }
      addStatAndOptionalPercentToReport( out, name, count, percent, "\t" );
	  
      if ( getShowIds() ) {
        // TODO: Only doing doc IDs of docs with no indexed terms
        // but technically we should also include those of missing Stored values
        // but realistically these will almost always be the same docs
        // and we don't do stored fields by default
        out.println( "\t\tIDs of non-indexed docs:" );
        addMissingIdsToReport( out, name, "\t\t\t" );
      }
    }
    
    // If Including Stored Fields
    if ( getIncludeStoredFields() ) {

      // Stored (vs. Indexed)

      if ( rows > 0 || start > 0 ) {
        // See also tabulateFieldsWithIndexedValues
        out.println();
        out.println( "WARNING: *Stored* Field statistics might only be ESTIMATES since samples were restricted:" );
        out.println( "\t--rows=" + rows + ", --start=" + start + ", see also --stored_fields" );
      }
      out.println();
      out.println( "Stored Values at 100%"
          + " (" + getFullyPopulatedStoredFields().size() + ")"
          + ": " + getFullyPopulatedStoredFields()
          );
      out.println();
      out.println( "No Stored Values / 0%"
          + " (" + getFieldsWithNoStoredValues().size() + ")"
          + ": " + getFieldsWithNoStoredValues()
          );
      out.println();
      // TODO: might be nice to sort by percent desc + name asc
      out.println( "Partially Stored Fields / Percentages:" );
      out.println( "\t(" + getPartiallyPopulatedStoredFields().size() + ")" );
      Set<String> lowFieldsStored = getPartiallyPopulatedStoredFields();
      for ( String name : lowFieldsStored ) {
        Long count = fieldStatsStoredValueCounts.get( name );
        Double percent = null;
        if ( fieldStatsStoredValuePercentages.containsKey(name) ) {
          percent = fieldStatsStoredValuePercentages.get( name );
        }
        // this shouldn't happen, parent loop shouldn't be requesting this field
        else if ( lukeStoredFields.contains(name) ) {
          percent = 0.0D;
        }
        addStatAndOptionalPercentToReport( out, name, count, percent, "\t" );
      }

      // Comparison / Joined
      out.println();
      out.println( "Comparision: Indexed and Stored Fields: (Indexed / Stored)" );
      if ( rows > 0 || start > 0 ) {
        // See also tabulateFieldsWithIndexedValues
        out.println( "\tREMINDER: Indexed stats are for all rows but Stored stats were restricted" );
      }
      Set<String> lowFieldsCombined = SetUtils.union_nonDestructive( lowFieldsIndexed, lowFieldsStored );
      out.println( "\t(" + lowFieldsCombined.size() + ")" );
      for ( String name : lowFieldsCombined ) {

        // Indexed
        Long indexedCount = fieldStatsIndexedValueCounts.get( name );
        if ( null==indexedCount && lukeIndexedFields.contains(name) ) {
          indexedCount = 0L;
        }
        Double indexedPercent = null;
        if ( fieldStatsIndexedValuePercentages.containsKey(name) ) {
          indexedPercent = fieldStatsIndexedValuePercentages.get( name );
        }
        // common scenario since loop uses combined list, distinguish between 0 and n/a
        else if ( lukeIndexedFields.contains(name) ) {
          indexedPercent = 0.0D;
        }

        // Stored
        Long storedCount = fieldStatsStoredValueCounts.get( name );
        if ( null==storedCount && lukeStoredFields.contains(name) ) {
          storedCount = 0L;
        }
        Double storedPercent = null;
        if ( fieldStatsStoredValuePercentages.containsKey(name) ) {
          storedPercent = fieldStatsStoredValuePercentages.get( name );
        }
        // common scenario since loop uses combined list, distinguish between 0 and n/a
        else if ( lukeStoredFields.contains(name) ) {
          storedPercent = 0.0D;
        }
        addDualStatsAndPercentsToReport( out, name, indexedCount, indexedPercent, storedCount, storedPercent, "\t" );
      }
    }  // End If Including Stored Fields

  }

  // TODO: this will break on certain data types like location/geo
  // TODO: only works on indexed values at this time
  void addMissingIdsToReport( PrintWriter out, String fieldName, String optIndent ) throws SolrServerException {
    Set<String> missingIds = SolrUtils.getEmptyFieldDocIds( solrServer, fieldName );
    for ( String id : missingIds ) {
      if ( null!=optIndent ) {
        out.print( optIndent );
      }
      out.println( "\"" + id + "\"" );
    }
  }

  void addSimpleStatToReport( PrintWriter out, String label, long stat ) {
    String statStr = NumberFormat.getNumberInstance().format( stat );
    out.println( "" + label + ": " + statStr );
  }
  void addStatAndOptionalPercentToReport( PrintWriter out, String label, long stat, Double optPerc ) {
    addStatAndOptionalPercentToReport( out, label, stat, optPerc, null );
  }
  void addStatAndOptionalPercentToReport( PrintWriter out, String label, long stat, Double optPerc, String optIndent ) {
    if ( null!=optIndent ) {
      out.print( optIndent );
    }
    String statStr = NumberFormat.getNumberInstance().format( stat );
    out.print( "" + label + ": " + statStr );
    if ( null!=optPerc ) {
      String fmtPerc = MessageFormat.format( "{0,number,#.##%}", optPerc );
      out.print( " (" + fmtPerc + ")" );
    }
    out.println();
  }
  void addDualStatsAndPercentsToReport( PrintWriter out, String label, Long statA, Double percA, Long statB, Double percB, String optIndent ) {
    if ( null!=optIndent ) {
      out.print( optIndent );
    }
    // statA = null==statA ? 0L : statA;
    // statB = null==statB ? 0L : statB;
    String statStrA = null;
    String statStrB = null;
    if ( null!=statA ) {
      statStrA = NumberFormat.getNumberInstance().format( statA );
    }
    else {
      statStrA = "-- n/a --";
    }
    if ( null!=statB ) {
      statStrB = NumberFormat.getNumberInstance().format( statB );
    }
    else {
      statStrB = "-- n/a --";
    }
    out.print( "" + label + ": ");
    out.print( statStrA );
    if ( null!=percA ) {
      String fmtPerc = MessageFormat.format( "{0,number,#.##%}", percA );
      out.print( " (" + fmtPerc + ")" );
    }
    out.print( " / ");
    out.print( statStrB );
    if ( null!=percB ) {
      String fmtPerc = MessageFormat.format( "{0,number,#.##%}", percB );
      out.print( " (" + fmtPerc + ")" );
    }
    out.println();
  }
  void _addFieldStatsToReport( PrintWriter out ) {
    for ( Entry<String, Long> entry : fieldStatsIndexedValueCounts.entrySet() ) {
      String name = entry.getKey();
      long stat = entry.getValue();
      // addSimpleStatToReport( out, name, stat );
      Double percent = null;
      if ( fieldStatsIndexedValuePercentages.containsKey(name) ) {
        percent = fieldStatsIndexedValuePercentages.get( name );
      }
      addStatAndOptionalPercentToReport( out, name, stat, percent );
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

  public static void main( String [] argv ) throws Exception {

    options = new Options();
    options.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
    options.addOption( "h", "host", true, "IP address for Solr, default=localhost" );
    options.addOption( "p", "port", true, "Port for Solr, default=8983" );
    options.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
    options.addOption( "s", "stored_fields", false, "Also check stats of Stored fields. WARNING: may take lots of time and memory for large collections" );
    options.addOption( "i", "ids", false, "Include IDs of docs with empty fields. WARNING: may create large report" );
    // TODO: could add option for number of IDs to include...
    options.addOption( "f", "fields", true, "Fields to analyze, Eg: fields=name,category, default is all fields" );

    // rows and start
    options.addOption(
        OptionBuilder
          .withLongOpt("rows")
          .withDescription( "Limit number of rows to check for *Stored* values, useful for low memory; doesn't affect Indexed stats. See also --stored_fields and --start" )
          .hasArg()
          .withType(Number.class) // NOT Long.class
          .create()
          );
    options.addOption(
        OptionBuilder
          .withLongOpt("start")
          .withDescription( "Offset of row to start checking *Stored* values, useful for low memory; doesn't affect Indexed stats. See also --stored_fields and --rows" )
          .hasArg()
          .withType(Number.class) // NOT Long.class
          .create()
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
    String fullUrl = cmd.getOptionValue( "url" );
    String host = cmd.getOptionValue( "host" );
    String port = cmd.getOptionValue( "port" );
    String coll = cmd.getOptionValue( "collection" );
    if ( null==fullUrl && null==host ) {
      helpAndExit( "Must specifify at least url or host", 3 );
    }
    if ( null!=fullUrl && null!=host ) {
      helpAndExit( "Must not specifify both url and host", 4 );
    }
    // Init
    // HttpSolrServer solr = SolrUtils.getServer( HOST, PORT, COLL );
    HttpSolrServer solr;
    if ( null!=fullUrl ) {
      solr = SolrUtils.getServer( fullUrl );
    }
    else {
      // Utils handle null values
      solr = SolrUtils.getServer( host, port, coll );    
    }
    // Options
    boolean includeStoredFields = false;
    if(cmd.hasOption("stored_fields")) {
      includeStoredFields = true;
    }
    boolean showIds = false;
    if(cmd.hasOption("ids")) {
      showIds = true;
    }
    Set<String> targetFields = null;
    String fieldsStr = cmd.getOptionValue( "fields" );
    if ( null!=fieldsStr ) {
      Set<String> fields = SetUtils.splitCsv( fieldsStr );
      if ( ! fields.isEmpty() ) {
        targetFields = fields;
      }
    }

    int rows = 0;
    // Don't use Integer
    Long rowsObj = (Long) cmd.getParsedOptionValue("rows");
    if (null != rowsObj) {
      if (rowsObj.intValue() <= 0) {
        helpAndExit("rows must be > 0", 5);
      }
      rows = rowsObj.intValue();
    }

    int start = 0;
    // Don't use Integer
    Long startObj = (Long) cmd.getParsedOptionValue("start");
    if (null != startObj) {
      if (startObj.intValue() <= 0) {
        helpAndExit("start must be > 0", 6);
      }
      start = startObj.intValue();
    }
    

    System.out.println( "Solr = " + solr.getBaseURL() );
    // EmptyFieldStats fs = new EmptyFieldStats( solr );
    EmptyFieldStats fs = new EmptyFieldStats( solr, targetFields, includeStoredFields, showIds, rows, start );

    String report = fs.generateReport( solr.getBaseURL() );
    System.out.println( report );

  }
}