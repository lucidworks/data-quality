package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
// import com.sun.tools.javac.util.List;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


// x import org.apache.commons.lang.CharUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;
import com.lucidworks.dq.util.StatsUtils;
import com.lucidworks.dq.util.CharUtils;

// Using composition, not inheritance, from EmptyFieldStats
public class TermCodepointStats /*implements HasDescription*/ {

  static String HOST = "localhost";
  static int PORT = 8983;
  // static String COLL = "collection1";
  static String COLL = "demo_shard1_replica1";
  
  static String HELP_WHAT_IS_IT = "Look for potentially corrupted tokens.  Assumption is corrupted data is more random and will therefore tend to span more Unicode classes.";
  static String HELP_USAGE = "TermCodepointStats";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  
  // final static Logger log = LoggerFactory.getLogger( TermCodepointStats.class );
  static Options options;

  // Object we're leveraging
  EmptyFieldStats fieldStats;

  boolean includeIndexedFields;
  int rows = 0;
  int start = 0;

  // Indexed Terms
  // -------------
  // fieldName -> term -> count
  Map<String, Map<String,Long>> rawTermsMap;
  // fieldName -> classifierTuple -> terms
  // Map<String,Map<Set<String>,Set<String>>> categorizedTerms;
  // Map<String,Map<List<String>,Set<String>>> categorizedTerms;
  Map<  String,  Map<String, Set<String>>  > categorizedTerms;
  
  // Stored Values
  // -------------
  // fieldName -> value -> count
  Map<String, Map<String,Long>> rawValuesMap;
  // fieldName -> classifierTuple -> values
  Map<  String,  Map<String, Set<String>>  > categorizedValues;

  public TermCodepointStats( HttpSolrServer server ) throws SolrServerException {
    this( server, null, null, null, 0, 0 );
  }
  public TermCodepointStats( HttpSolrServer server, Set<String> targetFields, Boolean includeIndexedFields, Boolean includeStoredFields, int rows, int start ) throws SolrServerException {
    this.rows = rows;
    this.start = start;
    // this.server = server;
    // this.fieldStats = new EmptyFieldStats( server );
    // Target Fields and includeStoredFields handled by fieldsStats  
    this.fieldStats = new EmptyFieldStats( server, targetFields, includeStoredFields, null, rows, start );

    // TODO: a bit of mismatch about Indexed Fields vs Stored Fields
    // In this class we let you turn off Indexed fields if you only want Stored fields
    // but the base/component class does understand about Stored fields
    if ( null==includeIndexedFields || includeIndexedFields.booleanValue() ) {
      this.includeIndexedFields = true;
    }

    // Sanity check
    if ( ! getIncludeIndexedFields() && ! getIncludeStoredFields() ) {
      throw new IllegalArgumentException( "Constructor Args: Must at least check Indexed or Stored fields, or both.  Otherwise nothing to do!" );
    }

    resetData( false );

    // TODO: should defer these?  Nice sanity check...
    // Don't chain the helper class since tey just did it
    doAllTabulations( false );

  }

  public HttpSolrServer getSolrServer() {
    return fieldStats.getSolrServer();
  }
  public Set<String> getTargetFields() {
    return fieldStats.getTargetFields();
  }
  public boolean getIncludeStoredFields() {
    return fieldStats.getIncludeStoredFields();
  }
  public boolean getIncludeIndexedFields() {
    return includeIndexedFields;
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
  public Set<String> getFieldsWithStoredValues() {
    return fieldStats.getFieldsWithStoredValues();
  }
  public Set<String> getFieldsWithNoStoredValues() {
    return fieldStats.getFieldsWithNoStoredValues();
  }

  public Set<String> _getFieldNamesWithTerms() {
    return rawTermsMap.keySet();
  }


  void resetData() throws SolrServerException {
    resetData( true );
  }
  void resetData( Boolean includeHelperClass ) throws SolrServerException {
    if ( null==includeHelperClass || includeHelperClass ) {
      fieldStats.resetData();
    }
    categorizedTerms = new TreeMap<>();
    rawTermsMap = new LinkedHashMap<>();
    categorizedValues = new TreeMap<>();
    rawValuesMap = new LinkedHashMap<>();
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
    // tabulateFields( getTargetFields() );
  }

  void tabulateAllFields() throws SolrServerException {
    // tabulateFields( getFieldNamesWithTerms() );

    if ( getIncludeIndexedFields() ) {
      if ( null!=getTargetFields() ) {
        tabulateIndexedFields( getTargetFields() );
      }
      else {
        tabulateIndexedFields( getFieldsWithIndexedValues() );
      }
    }
    if ( getIncludeStoredFields() ) {
      if ( null!=getTargetFields() ) {
        tabulateStoredFields( getTargetFields() );			
      }
      else {
        tabulateStoredFields( getFieldsWithStoredValues() );			
      }
    }

  }


  void tabulateIndexedFields( Set<String> fieldNames ) throws SolrServerException {
    System.out.println( "Fetching ALL terms, this may take a while ..." );
    long start = System.currentTimeMillis();

    // Includes Deleted Docs
    // Note: report includes statement about whether deleted docs are included or not
    // so if you change the impl, also change the report text
    rawTermsMap = SolrUtils.getAllTermsForFields_ViaTermsRequest( fieldStats.getSolrServer(), fieldNames );
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
      if ( rawTermsMap.containsKey(field) ) {
        Map<String,Long> terms = rawTermsMap.get(field);
        // Classify and tabulate terms into Unicode groupings
        // Map< Set<String>, Set<String> > classifications = classifyTerms( terms.keySet() );
        // Map< List<String>, Set<String> > classifications = classifyTerms( terms.keySet() );
        Map< String, Set<String> > classifications = classifyTerms( terms.keySet() );
        categorizedTerms.put( field, classifications );
      }
      // TODO: else... maybe override getFieldsWithNoIndexedValues
    }
  }

  void tabulateStoredFields( Set<String> fieldNames ) throws SolrServerException {
    System.out.println( "Fetching ALL values, this may take a while ..." );
    long startTime = System.currentTimeMillis();

    // Does NOT includes Deleted Docs
    // Note: report includes statement about whether deleted docs are included or not
    // so if you change the impl, also change the report text
    // rawTermsMap = SolrUtils.getAllTermsForFields_ViaTermsRequest( fieldStats.getSolrServer(), fieldNames );
    // Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getAllStoredValuesForFields_ByField( getSolrServer(), fieldNames );
    Map<String, Map<String, Collection<Object>>> docsByField = SolrUtils.getAllStoredValuesForFields_ByField( getSolrServer(), fieldNames, rows, start );
    rawValuesMap = SolrUtils.flattenStoredValues_ValueToTotalCount( docsByField );

    long stopTime = System.currentTimeMillis();
    long diff = stopTime - startTime;
    System.out.println( "Via Search Request took " + diff + " ms" );

    System.out.println( "Tabulating retrieved values ..." );
    // TODO: total term instances
    for ( String field : fieldNames ) {
      if ( rawValuesMap.containsKey(field) ) {
        Map<String,Long> values = rawValuesMap.get(field);
        Map< String, Set<String> > classifications = classifyTerms( values.keySet() );
        categorizedValues.put( field, classifications );
      }
    }
  }

  // classifierTuple -> terms
  // Map< Set<String>, Set<String> > classifyTerms( Set<String> terms )
  // Map< List<String>, Set<String> > classifyTerms( Set<String> terms )
  Map< String, Set<String> > classifyTerms( Set<String> terms )
  {
    // Map< Set<String>, Set<String> > out = new TreeMap<>();
    // Map< List<String>, Set<String> > out = new TreeMap<>();
    Map< String, Set<String> > out = new TreeMap<>();
    for ( String t : terms ) {
      // classificationKey -> charCount
      // Map<String,Long> termStats = CharUtils.classifyString_LongForm( t );
      Map<String,Long> termStats = CharUtils.classifyString_ShortForm( t );

      // Set<String> classificationTuple = termStats.keySet();
      // Set<String> classificationTuple = new LinkedHashSet<>( termStats.keySet() );
      // List<String> classificationTuple = new LinkedList<>( termStats.keySet() );
      // List<String> classificationTuple = Collections.unmodifiableList(  new LinkedList<>( termStats.keySet() )  );
      // List<String> classificationTuple = Collections.unmodifiableList(  new ArrayList<>( termStats.keySet() )  );
      List<String> classificationTuple = new ArrayList<>( termStats.keySet() );
      String classificationKey = SetUtils.join( classificationTuple );
      // if ( out.containsKey(classificationTuple) )
      // Set<String> value = out.get(classificationTuple);
      // if ( null!=value )
      if ( out.containsKey(classificationKey) )
      {
        // out.get(classificationTuple).add( t );
        // value.add( t );
        out.get(classificationKey).add( t );
      }
      else {
        // Preserve order of insertion
        Set<String> termVector = new LinkedHashSet<>();
        termVector.add( t );
        // out.put( classificationTuple, termVector );
        out.put( classificationKey, termVector );
      }
    }
    return out;
  }

  // TODO: could include label as a settable member field
  public String generateReport( String optLabel ) throws Exception {
    // *if* not done in constructor, nor by specific all,
    // then do it now
    // doAllTabulations();

    // TODO: compare, warn:
    // getTargetFieldNames()    - if null/empty, nothing to do
    // getAllFieldNames()       - if targets not here, Solr error
    // getFieldNamesWithTerms() - if targets not here, no term stats

    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    if ( null!=optLabel ) {
      out.println( "----------- " + optLabel + " -----------" );
    }
    addSimpleStatToReport( out, "Total Active Docs", getTotalDocCount(), null );

    out.println();
    out.println( "All Fields: " + getAllFieldNames() );

    // out.println();
    // out.println( "Target Fields to Analyze: " + getFieldNamesWithTerms() );
    if ( null!=getTargetFields() ) {
      out.println();
      out.println( "LIMITING to Target Fields: " + getTargetFields() );
    }

    if ( getIncludeIndexedFields() ) {
      out.println();
      // out.println( "Fields with Indexed Values: " + getFieldsWithIndexedValues() );
      // out.println( "Fields with Terms: " + getFieldNamesWithTerms() );
      out.println( "Fields with Indexed Terms: " + getFieldsWithIndexedValues() );

      out.println();
      // addAllFieldStatsToReport( out );
      // addFieldStatsToReport( out, fieldNames );
      addIndexedFieldStatsToReport( out );
    }

    if ( getIncludeStoredFields() ) {
      out.println();
      out.println( "Fields with Stored Values: " + getFieldsWithStoredValues() );
      out.println();
      addStoredFieldStatsToReport( out );
    }

    String outStr = sw.toString();
    return outStr;
  }
  void _addAllFieldStatsToReport( PrintWriter out ) {
    addIndexedFieldStatsToReport( out /*, getFieldNamesWithTerms()*/ );
  }
  void addIndexedFieldStatsToReport( PrintWriter out /*, Set<String> fieldNames*/ ) {
    addIndexedFieldStatsToReport( out, /* fieldNames,*/ 5 );
  }
  void addIndexedFieldStatsToReport( PrintWriter out, /*Set<String> fieldNames,*/ int sampleSliceSize ) {
    // Whether includes deleted or not controlled by tabulateFieldsWithIndexedValues
    out.println( "Indexed Terms Unicode Categories, for each Field (terms include deleted docs):" );
    // Foreach Field
    // for ( String field : fieldNames )
    for ( String field : rawTermsMap.keySet() )
    {
      out.println();
      out.println( "Field: " + field );
      // Map< Set<String>, Set<String> > stats = categorizedTerms.get( field );
      // Map< List<String>, Set<String> > stats = categorizedTerms.get( field );
      Map< String, Set<String> > stats = categorizedTerms.get( field );
      // for ( Entry< Set<String>, Set<String> > entry : stats.entrySet() )
      // for ( Entry< List<String>, Set<String> > entry : stats.entrySet() )
      for ( Entry< String, Set<String> > entry : stats.entrySet() )
      {
        // Set<String> tuple = entry.getKey();
        // List<String> tuple = entry.getKey();
        String tuple = entry.getKey();
        Set<String> terms = entry.getValue();
        out.println( "\tCharacter Classes: [" + tuple + "]" );
        int displayedCount = 0;
        boolean brokeEarly = false;
        for ( String t : terms ) {
          displayedCount++;
          if ( displayedCount > sampleSliceSize ) {
            brokeEarly = true;
            break;
          }
          out.println( "\t\t" + t );
        }
        if ( ! brokeEarly ) {
          out.println( "\t\t\t(showing all " + terms.size() + " terms)" );
        }
        else {
          out.println( "\t\t\t... (" + terms.size() + " terms)" );
        }
      }
    }
  }

  void addStoredFieldStatsToReport( PrintWriter out ) {
    addStoredFieldStatsToReport( out, 5 );
  }
  void addStoredFieldStatsToReport( PrintWriter out, int sampleSliceSize ) {
    // Whether includes deleted or not controlled by tabulateFieldsWithIndexedValues
    out.println( "Stored Values Unicode Categories, for each Field (values do not include deleted docs):" );
    // Foreach Field
    for ( String field : rawValuesMap.keySet() ) {
      out.println();
      out.println( "Field: " + field );
      Map< String, Set<String> > stats = categorizedValues.get( field );
      for ( Entry< String, Set<String> > entry : stats.entrySet() ) {
        String tuple = entry.getKey();
        Set<String> terms = entry.getValue();
        out.println( "\tCharacter Classes: [" + tuple + "]" );
        int displayedCount = 0;
        boolean brokeEarly = false;
        for ( String t : terms ) {
          displayedCount++;
          if ( displayedCount > sampleSliceSize ) {
            brokeEarly = true;
            break;
          }
          out.println( "\t\t" + t );
        }
        if ( ! brokeEarly ) {
          out.println( "\t\t\t(showing all " + terms.size() + " values)" );
        }
        else {
          out.println( "\t\t\t... (" + terms.size() + " values)" );
        }
      }
    }
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
    // here
    options = new Options();
    options.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
    options.addOption( "h", "host", true, "IP address for Solr, default=localhost" );
    options.addOption( "p", "port", true, "Port for Solr, default=8983" );
    options.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
    // TODO: adding IDs would be hard since we don't get those back from calls
    // AND searching for some tokens might have syntax error issues
    // options.addOption( "i", "ids", false, "Include IDs of docs when displaying sample values." );
    options.addOption( "s", "stored_fields", false, "Also check stats of Stored fields. WARNING: may take lots of time and memory for large collections" );
    options.addOption( "S", "no_stored_fields", false, "Don't check stats of Stored fields; this is the default." );
    // TODO: -i for --indexed_fields conflicts with -i for --ids for including IDs in EmptyFieldStats
    // but here we really might want to focus on only stored values
    options.addOption( "i", "indexed_fields", false, "Check stats of Indexed fields; this is the default." );
    options.addOption( "I", "no_indexed_fields", false, "Don't check stats of Indexed fields.  Used with --stored_fields to only get Stored Fields info." );
    // TODO: add option for sample size
    options.addOption( "f", "fields", true, "Fields to analyze, Eg: fields=name,category, default is all indexed/stored fields" );

    // rows and start
    options.addOption(
        OptionBuilder
          .withLongOpt("rows")
          .withDescription( "Limit number of rows to check for stored values; useful for low memory, see also start" )
          .hasArg()
          .withType(Number.class) // NOT Long.class
          .create()
          );
    options.addOption(
        OptionBuilder
          .withLongOpt("start")
          .withDescription( "Offset of row to start checking for stored values; useful for low memory, see also rows" )
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

    // Indexed fields, default is Yes
    Boolean indexedFieldsFlagObj = null;
    if( cmd.hasOption("indexed_fields")) {
      // Can't do yes and no for same option
      if( cmd.hasOption("no_indexed_fields")) {
        helpAndExit( "Can't specifify --indexed_fields AND --no_indexed_fields", 4 );
      }
      else {
        indexedFieldsFlagObj = new Boolean( true );
      }
    }
    else if( cmd.hasOption("no_indexed_fields")) {
      indexedFieldsFlagObj = new Boolean( false );
    }

    // Stored fields, default is No
    Boolean storedFieldsFlagObj = null;
    if(cmd.hasOption("stored_fields")) {
      // Can't do yes and no for same option
      if(cmd.hasOption("no_stored_fields")) {
        helpAndExit( "Can't specifify --stored_fields AND --no_stored_fields", 4 );
      }
      else {
        storedFieldsFlagObj = new Boolean( true );
      }
    }
    else if(cmd.hasOption("no_stored_fields")) {
      storedFieldsFlagObj = new Boolean( false );
    }

    // Sanity check
    // Also double checked in constructor since user might skip the main logic
    // Indexed, default is Yes
    boolean checkIndexedFields = null==indexedFieldsFlagObj || indexedFieldsFlagObj.booleanValue();
    // Stored, default is No
    boolean checkStoredFields = null!=storedFieldsFlagObj && storedFieldsFlagObj.booleanValue();
    if ( ! checkIndexedFields && ! checkStoredFields ) {
      helpAndExit( "Syntax: Must at least check Indexed or Stored fields, or both.  Otherwise nothing to do!", 4 );
    }


    String targetFieldsStr = cmd.getOptionValue( "fields" );
    // List<String> fieldNames = Arrays.asList( new String[]{"categoryNames", "class", "color", "department", "genre", "mpaaRating"} );
    Set<String> targetFields = null;
    if ( null!=targetFieldsStr ) {
      targetFields = SetUtils.splitCsv( targetFieldsStr );
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

    TermCodepointStats tcp = new TermCodepointStats( solr, targetFields, indexedFieldsFlagObj, storedFieldsFlagObj, rows, start );

    String report = tcp.generateReport( solr.getBaseURL() );
    System.out.println( report );
  }
}