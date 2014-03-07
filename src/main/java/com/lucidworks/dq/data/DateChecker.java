package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
// import com.sun.tools.javac.util.List;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.DateUtils;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;
import com.lucidworks.dq.util.StatsUtils;
import com.lucidworks.dq.util.HasDescription;

// Using composition, not inheritance, from EmptyFieldStats
public class DateChecker /*implements HasDescription*/ {

  static String HOST = "localhost";
  static int PORT = 8983;
  // static String COLL = "collection1";
  static String COLL = "demo_shard1_replica1";

  static String HELP_WHAT_IS_IT = "Look at the dates stored the collection.";
  static String HELP_USAGE = "DateChecker";
  // final static Logger log = LoggerFactory.getLogger( DateChecker.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  /***
   TODO: Best way to do calculations?
   # When to fetch and calculate:
     - At constructor time (not as easy to add new setters)
     - After ctor but before generating report
     - Automatically, all stats, when generate report is first called
     - On the fly / interleaved while generating report
   # Type of Solr query
     - Normal *:* query, full field scan
     - Stored values via Grouping / Field Collapsing
     - Indexed values via Faceting
     - Stats request handler
   # If showing sample docs
     - Gather up front or
     - Requery for each
   # Field-at-a-time or all at once
     - number of Solr transactions vs. Solr response size (memory)
   # Who does calculations
     - we do it here
     - done by Solr stats
   ***/
  
  
  static Options options;

  HttpSolrServer solrServer;
  Set<String> targetFields = null;
  Long totalDocs;
  Set<String> allFieldNames;

  Map<String, Map<Date,Long>> rawDatesMap_byField;
  Map<String,Long> uniqueDateCounts_byField;
  Map<String,Long> totalDateCounts_byField;
  Map<String,Date> dates_byField_min;
  Map<String,Date> dates_byField_max;
  Map<String,Date> totalDates_byField_avg;
  Map<String,Double> totalDates_byField_std;

  Map<String,Map<java.util.Date,Long>> dateHistogramByField;

  public DateChecker( HttpSolrServer server ) throws SolrServerException, java.text.ParseException {
	this( server, null );
  }
  // TODO: refactor to allow options to be settable after constructor is run
  public DateChecker( HttpSolrServer server, Set<String> targetFields ) throws SolrServerException, java.text.ParseException {
	this.solrServer = server;
	if ( null!=targetFields && ! targetFields.isEmpty() ) {
	  this.targetFields = targetFields;
	}

	// this.fieldStats = new EmptyFieldStats( server, targetFields, true, null );
    
	resetData( false );
	// TODO: should defer these?  Nice sanity check...
	// Don't chain the helper class since tey just did it
	doAllTabulations( false );
	// System.err.println( "Constructor: temp skipping call to doAllTabulations" );
  }
  public HttpSolrServer getSolrServer() {
	// return fieldStats.getSolrServer();
	return this.solrServer;
  }
  public Set<String> getTargetFields() {
	// return fieldStats.getTargetFields();
    return targetFields;
  }

  void fetchTotalDocCountFromServer() throws SolrServerException {
	totalDocs = SolrUtils.getTotalDocCount( getSolrServer() );
  }
  public long getTotalDocCount() {
	// return fieldStats.getTotalDocCount();
	return totalDocs;
  }
  public Set<String> getAllFieldNames() {
	// return fieldStats.getAllFieldNames();
	return allFieldNames;
  }
//  public Set<String> _getFieldsWithStoredValues() {
//	return fieldStats.getFieldsWithStoredValues();
//  }
//  public Set<String> _getFieldsWithNoStoredValues() {
//	return fieldStats.getFieldsWithNoStoredValues();
//  }

  // Small Solr demo collection: [incubationdate_dt, manufacturedate_dt]
  // Large BBuyOpen collection:  [releaseDate, startDate]
  // Values are usually null or java.util.Date
  public Set<String> guessDateFields() throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	Map<String, String> fields = SolrUtils.getLukeFieldTypes( getSolrServer() );
	for ( Entry<String, String> entry : fields.entrySet() ) {
	  String fieldName = entry.getKey();
	  String typeName = entry.getValue();
	  // TODO: replace with list of regexes
	  if ( typeName.toLowerCase().indexOf("date") >= 0 ) {
		out.add( fieldName );
		// System.out.println( "Date field \"" + fieldName + "\" of type \"" + typeName + "\"" );
	  }
	}
	return out;
  }
  
  // Stored Values
  public Map<String,Long> getUniqueDateCountsByField() {
	return uniqueDateCounts_byField;
  }
  public Map<String,Long> getTotalDateCountsByField() {
	return totalDateCounts_byField;
  }
  public Date getTotalDateCount_AverageForField( String fieldName ) {
	return totalDates_byField_avg.get( fieldName );
  }
  public Date getDate_MinForField( String fieldName ) {
	return dates_byField_min.get( fieldName );
  }
  public Date getDate_MaxForField( String fieldName ) {
	return dates_byField_max.get( fieldName );
  }
  public Set<String> _getFieldNamesWithValues() {
//	Set<String> out = new LinkedHashSet( uniqueValueCounts.keySet() );
//	return out;
	return null;
  }

  void resetData() throws SolrServerException {
	resetData( true );
  }
  void resetData( Boolean includeHelperClass ) throws SolrServerException {
//	if ( null==includeHelperClass || includeHelperClass ) {
//	  fieldStats.resetData();
//	}
	totalDocs = 0L;

	rawDatesMap_byField = new LinkedHashMap<>();
    uniqueDateCounts_byField = new LinkedHashMap<>();
    totalDateCounts_byField = new LinkedHashMap<>();
    dates_byField_min = new LinkedHashMap<>();
    dates_byField_max = new LinkedHashMap<>();
    totalDates_byField_avg = new LinkedHashMap<>();
    totalDates_byField_std = new LinkedHashMap<>();
    
    dateHistogramByField = new LinkedHashMap<>();

    allFieldNames = SolrUtils.getAllDeclaredAndActualFieldNames(solrServer);
  }


  
  void doAllTabulations() throws SolrServerException, java.text.ParseException {
	doAllTabulations( true );
  }
  // We can skip this only when we've first called their constructor
  // TODO: a bit awkward... requires knowledge of helper class impl
  void doAllTabulations( Boolean includeHelperClass ) throws SolrServerException, java.text.ParseException {
	fetchTotalDocCountFromServer();
	tabulateAllFields();
  }
  
  void tabulateAllFields() throws SolrServerException, java.text.ParseException {
    if ( null!=getTargetFields() ) {
      tabulateDates( getTargetFields() );			
	}
	else {
	  tabulateDates( guessDateFields() );			
	}
  }

  void tabulateDates( Set<String> fieldNames ) throws SolrServerException, java.text.ParseException {
    // dateHistogramByField
    for ( String field : fieldNames ) {
      // System.out.println( "Fetching field: " + field );
      Map<java.util.Date,Long> histo = SolrUtils.getHistogramForDateField( getSolrServer(), field, 1 );
      dateHistogramByField.put( field, histo );
    }
  
  }
  void _tabulateDates( Set<String> fieldNames ) throws SolrServerException, java.text.ParseException {
	System.out.println( "Fetching all date values, this may take a while ..." );
	// long start = System.currentTimeMillis();

	// Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getAllStoredValuesForFields_ByField( getSolrServer(), fieldNames );
	// valuesMap = SolrUtils.flattenStoredValues_ValueToTotalCount( docsByField );

	// long stop = System.currentTimeMillis();
	// long diff = stop - start;
	// System.out.println( "Via Search took " + diff + " ms" );
	
	// System.out.println( "Tabulating retrieved values ..." );
	// TODO: total term instances
	for ( String field : fieldNames ) {
	  System.out.println( "Fetching field: " + field );

	  long start = System.currentTimeMillis();

	  Map<Object,Long> generalVals = null;

	  // Try the much faster way first!
	  try {
	    // 1.2 M BBuy
	    // releaseDate: Mon Jul 10 17:00:00 PDT 2006 (1,224) is a java.lang.String
	    //    Unique / Total values: 8,212 / 1,275,077, in 463 ms
	    // startDate:   Mon Dec 12 16:00:00 PST 2005 (1,225) is a java.lang.String
	    //    Unique / Total values: 8,711 / 1,275,077, in 344 ms
	    start = System.currentTimeMillis();
	    generalVals = SolrUtils.getAllStoredValuesAndCountsForField_ViaGroupedQuery( getSolrServer(), field );
	  }
	  catch( Exception e ) {
		// Fallback, 100x slower
	    // releaseDate: Mon Jul 10 17:00:00 PDT 2006 (1,224 vals) is a java.util.Date
	    //   Unique / Total values: 8,212 / 1,275,077, in 41,716 ms
	    // startDate:   Mon Dec 12 16:00:00 PST 2005 (1,225 vals) is a java.util.Date
	    //   Unique / Total values: 8,711 / 1,275,077, in 41,368 ms
	    // approx 42 seconds
		start = System.currentTimeMillis();
		generalVals = SolrUtils.getAllStoredValuesAndCountsForField_ViaNormalQuery( getSolrServer(), field );
	  }  
	  long stop = System.currentTimeMillis();
	  long diff = stop - start;
	  System.out.print( "Number of Unique / Total values: " + generalVals.size() + " / " + StatsUtils.sumList_Longs(generalVals.values()) );
	  System.out.println( ", in " + diff + " ms" );

	  // Normalize to proper dates
	  Map<java.util.Date,Long> dateVals = new LinkedHashMap<>();
	  // TODO: might need to set locale/pattern if very different Solr machine locale
	  // Example: Thu Feb 20 20:29:25 PST 2014
	  // Format:  EEE MMM dd HH:mm:ss z yyyy
	  // DateFormat formatter = new SimpleDateFormat();
	  DateFormat formatter = new SimpleDateFormat( "EEE MMM dd HH:mm:ss z yyyy" );
	  Date now = new Date();

	  for ( Entry<Object, Long> tmpEntry : generalVals.entrySet() ) {
		Object keyObj = tmpEntry.getKey();
		Long count = tmpEntry.getValue();
		if ( null==keyObj ) {
		  dateVals.put( null, count );
		}
		else if ( keyObj instanceof java.util.Date ) {
		  java.util.Date newDate = (java.util.Date) keyObj;
		  dateVals.put( newDate, count );
          if ( newDate.getTime() < 0L || newDate.after(now) ) {
        	System.out.println( "Rogue DATE object " + newDate + " with " + count + " entries" );
          }
		}
		else if ( keyObj instanceof String ) {
		  java.util.Date newDate = formatter.parse( (String) keyObj );
		  dateVals.put( newDate, count );
          if ( newDate.getTime() < 0L || newDate.after(now) ) {
        	System.out.println( "Rogue string date, input=\"" +  (String) keyObj + "\", obj=\"" + newDate + "\", with " + count + " entries" );
          }
		}
	  }

	  // Now do proper calculations
	  int numUniqueDates = dateVals.size();
	  long numTotalDates = StatsUtils.sumList_Longs( dateVals.values() );
	  if ( dateVals.containsKey(null) ) {
		numUniqueDates -= 1;
		numTotalDates -= dateVals.get(null);
	  }
	  uniqueDateCounts_byField.put( field, new Long(numUniqueDates) );
	  totalDateCounts_byField.put( field, new Long(numTotalDates) );

	  // We want WEIGHTED averages
	  // though calculating both here since I'm already looping
	  long uniqueCount = 0L;
	  long uniqueSum = 0L;
	  long weightedCount = 0L;
	  long weightedSum = 0L;
	  Date minDate = null;
	  Date maxDate = null;
	  Date avgUniqueDate = null;
	  Date avgWeightedDate = null;
	  for ( Entry<Date, Long> dateEntry : dateVals.entrySet() ) {
		Date d = dateEntry.getKey();
		if ( null!=d ) {
		  if ( null==minDate || d.before(minDate) ) {
			minDate = d;
		  }
		  if ( null==maxDate || d.after(maxDate) ) {
			maxDate = d;
		  }
		  Long count = dateEntry.getValue();
		  uniqueCount += 1L;
		  weightedCount += count;
		  long ms = d.getTime();
		  uniqueSum += ms;
		  weightedSum += count * ms;
		}
	  }
	  if ( uniqueCount > 0L ) {
		Double avgMsD = (double) uniqueSum / (double) uniqueCount;
		long avgMsL = new Double( avgMsD + 0.5 ).longValue();
		avgUniqueDate = new Date( avgMsL );
	  }
	  if ( weightedCount > 0L ) {
		Double avgMsD = (double) weightedSum / (double) weightedCount;
		long avgMsL = new Double( avgMsD + 0.5 ).longValue();
		avgWeightedDate = new Date( avgMsL );
	  }
	  
	  System.out.println( "\tMin Date = " + minDate );
	  System.out.println( "\tWeighted Avg = " + avgWeightedDate );
	  System.out.println( "\tUnique Avg = " + avgUniqueDate );
	  System.out.println( "\tMax Date = " + maxDate );

//        // Value Lengths
//        if ( ! values.isEmpty() ) {
//		  Map<String,Integer> valueLengthVector = new LinkedHashMap<>();
//		  for ( String val : values.keySet() ) {
//		    valueLengthVector.put( val, val.length() );
//		  }
//		  int min = StatsUtils.minList_Ints( valueLengthVector.values() );
//		  uniqueValueLengths_byField_min.put( field, min );
//		  int max = StatsUtils.maxList_Ints( valueLengthVector.values() );
//		  uniqueValueLengths_byField_max.put( field, max );
//		  double avg = StatsUtils.averageList_Ints( valueLengthVector.values() );
//		  uniqueValueLengths_byField_avg.put( field, avg );
//		  double std = StatsUtils.standardDeviationList_Ints( valueLengthVector.values() );
//		  uniqueValueLengths_byField_std.put( field, std );
//        }

	  // TODO: else... maybe override getFieldsWithNoStoredValues
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

    if ( null!=getTargetFields() ) {
        out.println();
        out.println( "LIMITING to Target Fields: " + getTargetFields() );
    }

    out.println();
    // out.println( "Fields with Stored Values: " + getFieldNamesWithValues() );
    out.println();
    addDateStatsToReport( out );

    String outStr = sw.toString();
    return outStr;
  }

  void addDateStatsToReport( PrintWriter out ) {
    // Map<String,Map<java.util.Date,Long>> dateHistogramByField;
	for ( Entry<String, Map<Date, Long>> fieldEntry : dateHistogramByField.entrySet() ) {
	  String fieldName = fieldEntry.getKey();
	  Map<Date, Long> histo = fieldEntry.getValue();
	  // figure out the scale
	  int BAR_LEN = 60;
	  long maxCount = StatsUtils.maxList_Longs( histo.values() );
	  out.print( "Date Field: " + fieldName );
	  if ( maxCount <= 0L ) {
		out.print( "(No entries to Graph)" );
		return;
	  }
	  else {
        out.println();
	  }
	  float scale = (float) BAR_LEN / (float) maxCount;

	  List<Date> dates = new ArrayList<>( histo.keySet() );
	  List<Long> counts = new ArrayList<>( histo.values() );
	  if ( dates.size() != counts.size() ) {
	    throw new IllegalStateException( "Number of of keys (" + dates.size() + ") != number of values (" + counts.size() );
	  }
	  List<Double> x = DateUtils.dates2Doubles( dates );
	  List<Double> y = StatsUtils.longs2Doubles( counts );
	  // leastSquares_Exponential only looks at values > 0.0
	  double[] curve = StatsUtils.leastSquares_Exponential( x, y );
	  double A = curve[0];
	  double k = curve[1];  
	  // Reverse their order; we use LinkedHashMaps which preserves insertion order
	  histo = SetUtils.reverseMapEntryKeyOrder( histo );
	  // Plot the samples
	  for ( Entry<Date, Long> histoEntry : histo.entrySet() ) {
	    Date d = histoEntry.getKey();
	    Long v = histoEntry.getValue();
	    Double curvePoint = A * Math.exp( k * DateUtils.date2Double(d) );
	    addDateBarToReport( out, d, v, curvePoint, scale, "\t", true );
	  }
	}
  }

  void addDateBarToReport( PrintWriter out, Date date, long count, Double optCurvePoint, float scale, String optIndent, Boolean skipEmpty ) {
	// DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
	DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd" );
	// We don't display timezone, but don't want 2010 -> 2009 14:00 PST
	formatter.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
	String label = formatter.format( date );
	int numChars = new Double( (double)count * scale + 0.5 ).intValue();
    String bar = generateBar( "=", numChars );
    if ( bar.length() < 1 ) {
      if (null==skipEmpty || skipEmpty.booleanValue() ) {
    	return;
      }
    }
    if ( null!=optCurvePoint ) {
      int curveAt = new Double( optCurvePoint * scale + 0.5 ).intValue();
      bar = plotPointOnBar( bar, "|", "#", curveAt );
    }
	if ( null!=optIndent ) {
	  out.print( optIndent );
	}
	out.println( "" + label + ": " + bar );
  }
  static String generateBar( String s, int n ) {
	if ( n <= 0 ) { return ""; }
	// http://stackoverflow.com/questions/1235179/simple-way-to-repeat-a-string-in-java
	return new String(new char[n]).replace("\0", s);
  }
  static String plotPointOnBar( String oldBar, String newTextClear, String newTextOverlap, int positionBaseOne ) {
    // String newBar = oldBar + " (length was " + oldBar.length() + ", marker at " + positionBaseOne + ")";
    // return newBar;
	if ( positionBaseOne < 1 ) {
	  return oldBar;
	}
	int newBarLen = Math.max( oldBar.length(), positionBaseOne );
	StringBuffer newBar = new StringBuffer();
	for ( int i=1; i<=newBarLen; i++ ) {
	  String barChar = " ";
	  if ( i <= oldBar.length() ) {
		barChar = oldBar.substring( i-1, i );
	  }
	  if ( i == positionBaseOne ) {
		if ( barChar.equalsIgnoreCase(" ") ) {
		  barChar = newTextClear;
		}
		else {
		  barChar = newTextOverlap;
		}
	  }
	  newBar.append( barChar );
	}
	return new String( newBar );
  }
  void addSimpleStatToReport( PrintWriter out, String label, long stat ) {
	addSimpleStatToReport( out, label, stat, null );
  }
  void addSimpleStatToReport( PrintWriter out, String label, long stat, String optIndent ) {
	if ( null!=optIndent ) {
		out.print( optIndent );
	}
	String statStr = NumberFormat.getNumberInstance().format( stat );
	out.println( "" + label + ": " + statStr );
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
	System.out.println( "Current time in default format:" + new java.util.Date() );

	options = new Options();
	options.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
	options.addOption( "h", "host", true, "IP address for Solr, default=localhost" );
	options.addOption( "p", "port", true, "Port for Solr, default=8983" );
	options.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
	// TODO: adding IDs would be hard since we don't get those back from calls
	// AND searching for some tokens might have syntax error issues
	// options.addOption( "i", "ids", false, "Include IDs of docs when displaying sample values." );
	// TODO: add option for sample size
	options.addOption( "f", "fields", true, "Fields to analyze, Eg: fields=name,category, default is all indexed/stored fields" );
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

    String targetFieldsStr = cmd.getOptionValue( "fields" );
    Set<String> targetFields = null;
    if ( null!=targetFieldsStr ) {
      Set<String> fields = SetUtils.splitCsv( targetFieldsStr );
      if ( ! fields.isEmpty() ) {
    	targetFields = fields;
      }
    }

    DateChecker dc = new DateChecker( solr, targetFields );
    // System.out.println( "Date Fields: " + dc.guessDateFields() );
    // Small Solr demo collection: [incubationdate_dt, manufacturedate_dt]
    // Large BBuyOpen collection:  [releaseDate, startDate]

    String report = dc.generateReport( solr.getBaseURL() );
	System.out.println( report );

  }
}