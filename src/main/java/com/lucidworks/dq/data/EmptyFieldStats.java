package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.SolrUtils;

public class EmptyFieldStats {
  static String HOST = "localhost";
  static int PORT = 8983;
  // static String COLL = "collection1";
  static String COLL = "demo_shard1_replica1";

  static String HELP_WHAT_IS_IT = "Look for fields that aren't fully populated.";
  static String HELP_USAGE = "EmptyFieldStats -u http://localhost:8983";
  // final static Logger log = LoggerFactory.getLogger( FieldStats.class );

  static Options options;

  HttpSolrServer server;
  Long totalDocs;
  // TODO: could also do stored values
  Map<String,Long> fieldStatsIndexedValueCounts;
  Map<String,Long> fieldStatsIndexedValueDeficits;
  // We're using Longs; also I think Java uses Dboubles internally anyway
  Map<String,Double> fieldStatsIndexedValuePercentages;

  Set<String> fieldNames;
  Set<String> fieldsWithIndexedValues;
  Set<String> fieldsWithNoIndexedValues;
  // Only for Non-Zero collections
  Set<String> fullCountIndexedValues;
  Set<String> lowCountIndexedValues;

  public EmptyFieldStats( HttpSolrServer server ) throws SolrServerException {
	this.server = server;
	resetData();
	// TODO: should defer these?  Nice sanity check...
	doAllTabulations();
  }

  void resetData() throws SolrServerException {
	totalDocs = 0L;
    fieldStatsIndexedValueCounts = new LinkedHashMap<>();
    fieldStatsIndexedValueDeficits = new LinkedHashMap<>();
    fieldStatsIndexedValuePercentages = new LinkedHashMap<>();
    fieldsWithIndexedValues = new LinkedHashSet<>();
    fieldsWithNoIndexedValues = new LinkedHashSet<>();
    fullCountIndexedValues = new LinkedHashSet<>();
    lowCountIndexedValues = new LinkedHashSet<>();
    fieldNames = SolrUtils.getAllDeclaredAndActualFieldNames(server);
  }
  // TODO: change to public if we defer from constructor
  void doAllTabulations() throws SolrServerException {
	fetchTotalDocCountFromServer();
	// TODO: could skip some steps if 0 docs
	tabulateAllFields();
  }

  void fetchTotalDocCountFromServer() throws SolrServerException {
	totalDocs = SolrUtils.getTotalDocCount( getServer() );
  }
  public HttpSolrServer getServer() {
	return this.server;
  }
  public long getTotalDocCount() {
	return totalDocs;
  }

  // TODO: could make these unmodifiable
  public Set<String> getAllFieldNames() {
	return fieldNames;
  }
  public Set<String> getFieldsWithIndexedValues() {
	return fieldsWithIndexedValues;
  }
  public Set<String> getFieldsWithNoIndexedValues() {
	return fieldsWithNoIndexedValues;
  }
  // Only for Non-Empty Collection
  public Set<String> getFullyPopulatedIndexedFields() {
	return fullCountIndexedValues;
  }
  // Only for Non-Empty Collection
  public Set<String> getPartiallyPopulatedIndexedFields() {
    return lowCountIndexedValues;
  }

  public Map<String,Long> getIndexedValueCounts() {
	  return fieldStatsIndexedValueCounts;
  }
  public Map<String,Double> getIndexedValuePercentages() {
	  return fieldStatsIndexedValuePercentages;
  }

  void tabulateAllFields() throws SolrServerException {
	tabulateFieldsWithIndexedValues( fieldNames );
  }
  void tabulateFieldsWithIndexedValues( Set<String> fieldNames ) throws SolrServerException {
	for ( String field : fieldNames ) {
	  long stat = SolrUtils.getDocCountForField( getServer(), field );
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
    out.println( "All Fields: " + getAllFieldNames() );

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
  void addAllFieldStatsToReport( PrintWriter out ) {
    out.println( "Populated at 100%: " + getFullyPopulatedIndexedFields() );
    out.println();
    out.println( "No Indexed Values / 0%: " + getFieldsWithNoIndexedValues() );
    out.println();
    // TODO: might be nice to sort by percent desc + name asc
    out.println( "Partially Populated Fields / Percentages:" );
    Set<String> lowFields = getPartiallyPopulatedIndexedFields();
    for ( String name : lowFields ) {
      Long count = fieldStatsIndexedValueCounts.get( name );
	  Double percent = null;
	  if ( fieldStatsIndexedValuePercentages.containsKey(name) ) {
		  percent = fieldStatsIndexedValuePercentages.get( name );
	  }
	  addStatAndOptionalPercentToReport( out, name, count, percent, "\t" );
    }
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
	// options.addOption( "i", "ids", false, "Include IDs of docs with empty fields. WARNING: may create large report" );
	// options.addOption( "f", "fields", true, "Fields to analyze, Eg: fields=name,category, default is all fields" );
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
    System.out.println( "Solr = " + solr.getBaseURL() );
	EmptyFieldStats fs = new EmptyFieldStats( solr );
	String report = fs.generateReport( solr.getBaseURL() );
	System.out.println( report );
  
  }
}