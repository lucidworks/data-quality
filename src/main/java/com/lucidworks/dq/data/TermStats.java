package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
// import com.sun.tools.javac.util.List;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;
import com.lucidworks.dq.util.StatsUtils;

// Using composition, not inheritance, from EmptyFieldStats
public class TermStats {

  static String HOST = "localhost";
  static int PORT = 8983;
  // static String COLL = "collection1";
  static String COLL = "demo_shard1_replica1";

  static String HELP_WHAT_IS_IT = "Look at indexed tokens and lengths lengths in each field.";
  static String HELP_USAGE = "TermStats";
  // final static Logger log = LoggerFactory.getLogger( TermStats.class );

  static Options options;

  EmptyFieldStats fieldStats;

  Map<String, Map<String,Long>> termsMap;
  Map<String,Long> uniqueTermCounts;
  Map<String,Long> totalTermCounts;
  Map<String,Integer> uniqueTermLengths_byField_min;
  Map<String,Integer> uniqueTermLengths_byField_max;
  Map<String,Double> uniqueTermLengths_byField_avg;
  Map<String,Double> uniqueTermLengths_byField_std;
  
  public TermStats( HttpSolrServer server ) throws SolrServerException {
	// this.server = server;
	this.fieldStats = new EmptyFieldStats( server );
	resetData( false );
	// TODO: should defer these?  Nice sanity check...
	// Don't chain the helper class since tey just did it
	doAllTabulations( false );
  }

  void resetData() throws SolrServerException {
	resetData( true );
  }
  void resetData( Boolean includeHelperClass ) throws SolrServerException {
	if ( null==includeHelperClass || includeHelperClass ) {
	  fieldStats.resetData();
	}
	termsMap = new LinkedHashMap<>();
    uniqueTermCounts = new LinkedHashMap<>();
    totalTermCounts = new LinkedHashMap<>();
    uniqueTermLengths_byField_min = new LinkedHashMap<>();
    uniqueTermLengths_byField_max = new LinkedHashMap<>();
    uniqueTermLengths_byField_avg = new LinkedHashMap<>();
    uniqueTermLengths_byField_std = new LinkedHashMap<>();
  }
  void doAllTabulations() throws SolrServerException {
	doAllTabulations( true );
  }
  // We can skip this only when we've first called their constructor
  // TODO: a bit awkward... requires knowledge of helper class impl
  void doAllTabulations( Boolean includeHelperClass ) throws SolrServerException {
	if ( null==includeHelperClass || includeHelperClass ) {
	  fieldStats.doAllTabulations();
	}
	tabulateAllFields();
  }

  // Passthroughs to Helper class
  public long getTotalDocCount() {
	return fieldStats.getTotalDocCount();
  }
  public Set<String> getAllFieldNames() {
	return fieldStats.getAllFieldNames();
  }
  public Set<String> getFieldsWithIndexedValues() {
	return fieldStats.getFieldsWithIndexedValues();
  }
  public Set<String> getFieldsWithNoIndexedValues() {
	return fieldStats.getFieldsWithNoIndexedValues();
  }

  public Map<String,Long> getUniqueTermCountsByField() {
	return uniqueTermCounts;
  }
  public Map<String,Long> getTotalTermCountsByField() {
	return totalTermCounts;
  }
  public Double getUniqueTermCount_AverageLengthForField( String fieldName ) {
	return uniqueTermLengths_byField_avg.get( fieldName );
  }
  public Integer getUniqueTermCount_MinLengthForField( String fieldName ) {
	return uniqueTermLengths_byField_min.get( fieldName );
  }
  public Integer getUniqueTermCount_MaxLengthForField( String fieldName ) {
	return uniqueTermLengths_byField_max.get( fieldName );
  }
  public Set<String> getFieldNamesWithTerms() {
	Set<String> out = new LinkedHashSet( uniqueTermCounts.keySet() );
	return out;
  }

  void tabulateAllFields() throws SolrServerException {
	// tabulateFieldsWithIndexedValues( getAllFieldNames() );
	tabulateFieldsWithIndexedValues( getFieldsWithIndexedValues() );
  }
  void tabulateFieldsWithIndexedValues( Set<String> fieldNames ) throws SolrServerException {
	System.out.println( "Fetching ALL terms, this may take a while ..." );
	long start = System.currentTimeMillis();

	// Includes Deleted Docs
	// Note: report includes statement about whether deleted docs are included or not
	// so if you change the impl, also change the report text
	termsMap = SolrUtils.getAllTermsForFields_ViaTermsRequest( fieldStats.getServer(), fieldNames );
	long stop = System.currentTimeMillis();
	long diff = stop - start;
	System.out.println( "Via TermsRequest took " + diff + " ms" );

	// Try to get just active docs ...
	// ... slow, unstable ...
	// termsMap = SolrUtils.getTermsForFields_ViaSearchFacets( fieldStats.getServer(), fieldNames, -1 );
	// termsMap = SolrUtils.getTermsForFields_ViaSearchFacets( fieldStats.getServer(), fieldNames, 100 );
	// Set<String> tmpFieldNames = new LinkedHashSet<>();
	// tmpFieldNames.addAll( Arrays.asList(new String[]{ "class", "mpaaRating", "type" }) );
	// termsMap = SolrUtils.getTermsForFields_ViaSearchFacets( fieldStats.getServer(), tmpFieldNames, 100 );
	// termsMap = SolrUtils.getTermsForFields_ViaSearchFacets( fieldStats.getServer(), tmpFieldNames, -1 );
	// long stop = System.currentTimeMillis();
	// long diff = stop - start;
	// System.out.println( "Via SearchFacets took " + diff + " ms" );
	
	System.out.println( "Tabulating retrieved terms ..." );
	// TODO: total term instances
	for ( String field : fieldNames ) {
	  if ( termsMap.containsKey(field) ) {
		Map<String,Long> terms = termsMap.get(field);
		Long unqiueTermCount = new Long( terms.size() );
		Long totalTermCount = StatsUtils.sumList_Longs( terms.values() );
        uniqueTermCounts.put( field, unqiueTermCount );
        totalTermCounts.put( field, totalTermCount );
        // Term Lengths
        if ( ! terms.isEmpty() ) {
		  Map<String,Integer> termLengthVector = new LinkedHashMap<>();
		  for ( String term : terms.keySet() ) {
		    termLengthVector.put( term, term.length() );
		  }
		  int min = StatsUtils.minList_Ints( termLengthVector.values() );
		  uniqueTermLengths_byField_min.put( field, min );
		  int max = StatsUtils.maxList_Ints( termLengthVector.values() );
		  uniqueTermLengths_byField_max.put( field, max );
		  double avg = StatsUtils.averageList_Ints( termLengthVector.values() );
		  uniqueTermLengths_byField_avg.put( field, avg );
		  double std = StatsUtils.standardDeviationList_Ints( termLengthVector.values() );
		  uniqueTermLengths_byField_std.put( field, std );
        }
	  }
	  // TODO: else... maybe override getFieldsWithNoIndexedValues
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
    addSimpleStatToReport( out, "Total Active Docs", getTotalDocCount(), null );

    out.println();
    out.println( "All Fields: " + getAllFieldNames() );

    // out.println( "Fields with Indexed Values: " + getFieldsWithIndexedValues() );
    out.println( "Fields with Terms: " + getFieldNamesWithTerms() );

    out.println();
    addAllFieldStatsToReport( out );
    
    String outStr = sw.toString();
    return outStr;
  }
  void addSimpleStatToReport( PrintWriter out, String label, long stat, String optIndent ) {
	if ( null!=optIndent ) {
		out.print( optIndent );
	}
	String statStr = NumberFormat.getNumberInstance().format( stat );
	out.println( "" + label + ": " + statStr );
  }
  void addStatPairToReport( PrintWriter out, String label, long statA, long statB, String optIndent ) {
	if ( null!=optIndent ) {
		out.print( optIndent );
	}
	String statAStr = NumberFormat.getNumberInstance().format( statA );
	String statBStr = NumberFormat.getNumberInstance().format( statB );
	out.println( "" + label + ": " + statAStr + " / " + statBStr );
  }
  void addAllFieldStatsToReport( PrintWriter out ) {
	addTermStatsToReport( out );
	addTermLengthStatsToReport( out );
  }
  void addTermStatsToReport( PrintWriter out ) {
	addTermStatsToReport( out, 5 );
  }
  void addTermStatsToReport( PrintWriter out, int sampleSliceSize ) {
	// Whether includes deleted or not controlled by tabulateFieldsWithIndexedValues
	out.println( "Term Counts by Field, Unique / Total-instances (counts include deleted docs):" );
	// Foreach Field
	for ( Entry<String, Long> entry : uniqueTermCounts.entrySet() ) {
	  String fieldName = entry.getKey();
	  long uniqueTermCount = entry.getValue();
	  long totalTermCount = totalTermCounts.get( fieldName );
	  // addSimpleStatToReport( out, name, stat, "\t" );
	  addStatPairToReport( out, fieldName, uniqueTermCount, totalTermCount, "\t" );

	  // Term Lengths
	  // uniqueTermLengths_byField_avg
	  
	  addTermListSamplesToReport( out, fieldName, sampleSliceSize, "\t\t" );
	}
  }
  void addTermLengthStatsToReport( PrintWriter out ) {
	addTermLengthStatsToReport( out, 5 );
  }
  void addTermLengthStatsToReport( PrintWriter out, int sampleSliceSize ) {
	// Whether includes deleted or not controlled by tabulateFieldsWithIndexedValues
	out.println( "Unique Term Length Stats by Field, min/max/avg/std (terms include deleted docs):" );
	// Foreach Field
	for ( String fieldName : uniqueTermCounts.keySet() ) {
	  int min = uniqueTermLengths_byField_min.get( fieldName );
  	  String minStr = NumberFormat.getNumberInstance().format( min );
	  int max = uniqueTermLengths_byField_max.get( fieldName );
  	  String maxStr = NumberFormat.getNumberInstance().format( max );
	  double avg = uniqueTermLengths_byField_avg.get( fieldName );
	  DecimalFormat formatter = new DecimalFormat("#,##0.0#");
  	  String avgStr = formatter.format(avg);
	  double std = uniqueTermLengths_byField_std.get( fieldName );
	  String stdStr = formatter.format(std);
	  long numEntries = uniqueTermCounts.get( fieldName );
	  String numEntriesStr = NumberFormat.getNumberInstance().format( numEntries );
	  out.println( "\t" + fieldName + ": "
			  + minStr + " / " + maxStr + " / " + avgStr + " / " + stdStr
			  + "   (" + numEntriesStr + " entries)"
			  );
	  double threeSigma = (double) 3 * std;
	  int expectedMin = new Double( Math.floor( avg - threeSigma ) ).intValue();
	  int expectedMax = new Double( Math.ceil( avg + threeSigma ) ).intValue();
	  out.println( "\t\tExpected Length Range, raw: " + expectedMin + " to " + expectedMax + " (inclusive)" );
	  if ( expectedMin < min ) expectedMin = min;
	  if ( expectedMax > max ) expectedMax = max;
	  out.println( "\t\tExpected Length Range, clamped: " + expectedMin + " to " + expectedMax + " (inclusive)" );
	  // Underweight
	  if ( min < expectedMin ) {
		Map<String,Long> terms = termsMap.get( fieldName );
	    out.println( "\t\tUnusually Short Terms:" );
		int displayedSamples = 0;
		// Whether to show "..."
		boolean hadMore = false;
		int i = 0;
		// Do until we run out of terms OR have displayed enough examples
		for ( Entry<String, Long> entry : terms.entrySet() ) {
		  i++;
		  String term = entry.getKey();
		  if ( term.length() < expectedMin ) {
		    if ( displayedSamples >= sampleSliceSize ) {
			  hadMore = true;
			  break;
		    }
		    Long count = entry.getValue();
		    String countStr = NumberFormat.getNumberInstance().format( count );
		    String iStr = NumberFormat.getNumberInstance().format( i+1 );
		    out.println( "\t\t\t" + iStr + ": " + term + ", len=" + term.length() );
		    displayedSamples++;
		  }
		}
		if ( hadMore ) {
		  out.println( "\t\t\t..." );			
		}
	  }
	  // Overweight
	  if ( max > expectedMax ) {
		Map<String,Long> terms = termsMap.get( fieldName );
	    out.println( "\t\tUnusually Long Terms:" );
		int displayedSamples = 0;
		// Whether to show "..."
		boolean hadMore = false;
		int i = 0;
		// Do until we run out of terms OR have displayed enough examples
		for ( Entry<String, Long> entry : terms.entrySet() ) {
		  i++;
		  String term = entry.getKey();
		  if ( term.length() > expectedMax ) {
		    if ( displayedSamples >= sampleSliceSize ) {
			  hadMore = true;
			  break;
		    }
		    Long count = entry.getValue();
		    String countStr = NumberFormat.getNumberInstance().format( count );
		    String iStr = NumberFormat.getNumberInstance().format( i+1 );
		    out.println( "\t\t\t" + iStr + ": " + term + ", len=" + term.length() );
		    displayedSamples++;
		  }
		}
		if ( hadMore ) {
		  out.println( "\t\t\t..." );			
		}
	  }
	}
  }
  void addTermListSamplesToReport( PrintWriter out, String fieldName, int sampleSliceSize, String optIndent ) {
	  Map<String,Long> terms = termsMap.get( fieldName );
	  if ( terms.size() < (sampleSliceSize*3)+3 ) {
		int i = 0;
		for ( Entry<String, Long> entry : terms.entrySet() ) {
		  i++;
	      String term = entry.getKey();
	      Long count = entry.getValue();
      	  String countStr = NumberFormat.getNumberInstance().format( count );
	      if ( null!=optIndent ) {
	    	out.print( optIndent );
	      }
	      out.println( "" + i + ": " + term + " " + countStr );
		}
	  }
	  else {
	    // Works because we unse underlying LinkedHashSet and LinkedHashMap
	    List<String> termList = new ArrayList<>( terms.keySet() );
	    List<Long> termCounts = new ArrayList<>( terms.values() );
	    // Top Slice
	    addSliceToReport( out, 0, sampleSliceSize, termList, termCounts, optIndent );
	    if ( null!=optIndent ) out.print( optIndent );
	    out.println( "...");
	    // Mid Slice
	    int from = ( terms.size() - sampleSliceSize ) / 2;
	    int to = from + sampleSliceSize;
	    addSliceToReport( out, from, to, termList, termCounts, optIndent );
	    if ( null!=optIndent ) out.print( optIndent );
	    out.println( "...");
	    // Final Slice
	    from = terms.size() - sampleSliceSize;
	    to = terms.size();
	    addSliceToReport( out, from, to, termList, termCounts, optIndent );
	  }
  }
  void addSliceToReport( PrintWriter out, int from, int to, List<String> terms, List<Long> counts, String optIndent ) {
	for ( int i=from; i<to; i++ ) {
      String term = terms.get( i );
	  Long count = counts.get( i );
	  String countStr = NumberFormat.getNumberInstance().format( count );
	  String iStr = NumberFormat.getNumberInstance().format( i+1 );
      if ( null!=optIndent ) {
	    out.print( optIndent );
	  }
	  out.println( iStr + ": " + term + " " + countStr );
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
	// TODO: adding IDs would be hard since we don't get those back from calls
	// AND searching for some tokens might have syntax error issues
	// options.addOption( "i", "ids", false, "Include IDs of docs when displaying sample values." );
	// options.addOption( "s", "stored-fields", false, "Also check stats of Stored fields. WARNING: may take lots of time and memory for large collections" );
	// TODO: add option for sample size
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
    TermStats ts = new TermStats( solr );

    String report = ts.generateReport( solr.getBaseURL() );
	System.out.println( report );

  }
}