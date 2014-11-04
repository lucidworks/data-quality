package com.lucidworks.dq.logs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.StringUtils;

public class LogEntryFromSolr extends LogEntryBase {

  // Two types of lines:
  //
  // core.2014_10_10.log
  // 2014-10-10 00:05:50,525 INFO core.SolrCore - [DT_Products_CERT] webapp= path=/select params={pf=product_short_name^2+product_long_name^2+brand_name^1+sub_brand_name^1+tier1_names^1+tier2_names^1+tier3_names^1+tier4_names^1+product_description^2+text_all^1&sort=sales_rank+asc,&indent=false&q=*:*&qf=product_short_name^20+product_long_name^5+brand_name^10+sub_brand_name^10+tier1_names^5+tier2_names^5+tier3_names^5+tier4_names^5+product_description^2+text_all^1&q.alt=*:*&wt=json&fq=store_id:(32)&rows=30&defType=edismax} hits=24177 status=0 QTime=0  
  //
  // core.request.2014_10_13.log
  // 10.150.226.22 -  -  [13/Oct/2014:12:19:49 +0000] "GET /solr/DT_Products_CERT/select?q.alt=*:*&defType=edismax&q=%2A%3A%2A&fq=upc:(12364)&facet=true&facet.field=availability&f.availability.facet.sort=c&facet.field=ways_to_save&f.ways_to_save.facet.sort=c&facet.range=price_value&f.price_value.facet.range.start=0&f.price_value.facet.range.end=1000&f.price_value.facet.range.gap=50&facet.field=tier2_facets&f.tier2_facets.facet.sort=c&fl=upc%2Cupc_typ_ct%2Cid%2Cscore%2Cstore_id%2Cproduct_id%2Cproduct_short_name%2Cproduct_long_name%2Cproduct_description%2Cuom%2Cselling_size%2Cis_random_weight%2Cimages%2Cbrand_name%2Csub_brand_name%2Cbrand_is_meijer%2Cis_active%2Cis_sellable%2Cis_killed%2Cis_click_and_collect%2Cis_ship_to_home%2Cis_quick_shop%2Cis_alcohol%2Cis_tobacco%2Cis_age_restricted%2Cis_hazardous_material%2Cis_prepared_item%2Cis_organic%2Cis_primary_upc%2Cprice_text%2Cprice_value%2Csale_price_value%2Csale_price_text%2Csavings_value%2Csavings_value_text%2Chas_mperks_ofers%2Cis_local%2Ccool%2Cnutr_calories%2Cnutr_protien%2Cnutr_carbs%2Cmutr_fat%2Clast_updated%2Clast_updated_by%2Cweight_each%2Chot_item_flag%2Cis_substitutable%2Ctier1_ids%2Ctier2_ids%2Ctier3_ids%2Ctier4_ids%2Ctier1_facets%2Ctier2_facets%2Ctier3_facets%2Ctier4_facets%2Ctier1_names%2Ctier2_names%2Ctier3_names%2Ctier4_names%2Cmperks_offers&sort=sales_rank%20asc,&qf=product_short_name%5E20%20product_long_name%5E5%20brand_name%5E10%20sub_brand_name%5E10%20tier1_names%5E5%20tier2_names%5E5%20tier3_names%5E5%20tier4_names%5E5%20product_description%5E2%20text_all%5E1&pf=product_short_name%5E2%20product_long_name%5E2%20brand_name%5E1%20sub_brand_name%5E1%20tier1_names%5E1%20tier2_names%5E1%20tier3_names%5E1%20tier4_names%5E1%20product_description%5E2%20text_all%5E1&wt=json&indent=false HTTP/1.1" 200 2646  16
  
  // We overwrite rawText with params match
  String originalText;

  /*
   * When the constructor is called, we don't know for sure whether
   * this is a Solr log entry or not.
   * Although constructor is obligated to return an object,
   * the static factory will return null if we aren't.
   */
  boolean isSolrPattern = false;

  /*
   * If we're created from a less specific log entry
   */
  LogEntry earlierEntry;

  // Match params={q=...&...}
  static String PARAMS_PATTERN_STR = "params=[{]([^}]+)[}]";
  Pattern paramsPattern;
  Matcher paramsMatcher;

  // Match path=/handler_name
  // "Illegal repetition"
  static String HANDLER_PATTERN_STR = " path=([^ ]+) ";
  String handlerName;

  // LWS:  - [DT_Products_CERT] webapp=
  static String COLLECTION_PATTERN_STR = " - \\[([^\\]]+)\\] webapp=";
  String collectionName;

  // hits=19644 status=0 QTime=141
  static String HITS_PATTERN_STR = " hits=([0-9]+)";
  static String STATUS_PATTERN_STR = " status=([0-9]+)";
  static String QTIME_PATTERN_STR = " QTime=([0-9]+)";
  // Don't really need Longs here, but it's what utility returns
  Long hits;
  Long status;
  Long qTime;
  
  String paramsString;
  int paramsStart = -1;
  int paramsEnd = -1;
  
  Map<String,Collection<String>> parsedParamValues;

  // factory method
  public static LogEntry solrLogEntryFromBaseEntryOrNull( LogEntry entry ) {
    LogEntryFromSolr newEntry = new LogEntryFromSolr( entry );
    if ( newEntry.isSolrPattern() ) {
      return newEntry;
    }
    else {
      return null;
    }
  }

  LogEntryFromSolr( LogEntry entry ) {
    this( entry.getRawText() );
    this.earlierEntry = entry;
    init( entry.getRawText() );
  }
  LogEntryFromSolr(String rawText) {
    super( rawText );
    init( rawText );
  }
  // need init broken out so constructor1 can store earlierEntry before calling this
  void init( String rawText ) {
    this.originalText = rawText;
    paramsPattern = Pattern.compile( PARAMS_PATTERN_STR );
    paramsMatcher = paramsPattern.matcher( rawText );
    if ( paramsMatcher.find() ) {
      String matchStr = paramsMatcher.group();
      setRawText( matchStr );
      int overallStart = paramsMatcher.start();
      int overallEnd = paramsMatcher.end();

      int group = 1;
      paramsString = paramsMatcher.group( group );
      paramsStart = paramsMatcher.start( group );
      paramsEnd = paramsMatcher.end( group );
      // Make relative to overall pattern match
      paramsStart -= overallStart;
      // paramsEnd = overallEnd - paramsEnd;
      // Relative-to-end might not work in streaming apps since we wouldn't know where the end is
      paramsEnd -= overallStart;

      
      // TODO: look for other things like the handler, matches and qtime
      
      // Hookup references *if* we were created from an earlier log entry
      if ( null != this.earlierEntry ) {
        LogEntryReference ref = new LogEntryReferenceBase( this.earlierEntry, this, "LogEntryFromSolr" );
        // ((LogEntryReferenceBase) ref).setRelativeRegionOfInterest( paramsStart, paramsEnd );
        ((LogEntryReferenceBase) ref).setRelativeRegionOfInterest( overallStart, overallEnd );
      }

      doSimpleFieldParsing();

      isSolrPattern = true;
    }
  }

  public String makeParamNamesKey() {
    return StringUtils.join( getParsedSolrParams().keySet(), "|" );
  }
  public Set<String> getParamNames() {
    return getParsedSolrParams().keySet();
  }
  public Collection<String> getParamValues( String paramName ) {
    return getParsedSolrParams().get( paramName );
  }

  public static Map<String,Long> tabulateQueryArgCombos( Collection<LogEntryFromSolr> entries ) {
    Map<String,Long> counts = new HashMap<>();
    for ( LogEntryFromSolr e : entries ) {
      String key = e.makeParamNamesKey();
      SetUtils.incrementMapCounter( counts, key );
    }
    return counts;
  }
  // { composite-parameter-key -> { each-parameter-name-> { unique-value: count } } }
  public static Map<String,Map<String,Map<String,Long>>> tabulateQueryArgCombosAndValues( Collection<LogEntryFromSolr> entries ) {
    // Level 1: by Composite Key
    Map<String,Map<String,Map<String,Long>>> nestedCounts = new HashMap<>();
    // Foreach Raw Entry
    for ( LogEntryFromSolr e : entries ) {

      String overallKey = e.makeParamNamesKey();
      // Level 2: by Parameter Name
      Map<String,Map<String,Long>> paramsAndValues = null;
      if ( nestedCounts.containsKey(overallKey) ) {
        paramsAndValues = nestedCounts.get(overallKey);
      }
      else {
        paramsAndValues = new TreeMap<>(); // LinkedHashMap<>();
        nestedCounts.put( overallKey, paramsAndValues );
      }

      Set<String> paramNames = e.getParamNames();
      // Foreach Parameter Name
      for ( String name : paramNames ) {
        // Level 3: by Value
        Map<String,Long> tabulatedValues = null;
        if ( paramsAndValues.containsKey(name) ) {
          tabulatedValues = paramsAndValues.get(name);
        }
        else {
          tabulatedValues = new LinkedHashMap<>();
          paramsAndValues.put( name, tabulatedValues );
        }
        Collection<String> rawValues = e.getParamValues( name );
        for ( String rv : rawValues ) {
          Long count = 0L;
          if ( tabulatedValues.containsKey(rv) ) {
            count = tabulatedValues.get(rv);
          }
          count += 1L;
          tabulatedValues.put( rv, count );
        }

      }  // End Foreach Parameter Name
      
    }  // End Foreach Raw Entry

    return nestedCounts;
  }

  void doSimpleFieldParsing() {
    parseHandlerName();
    parseCollectionName();
    parseHits();
    parseStatus();
    parseQTime();
  }
  void parseHandlerName() {
    handlerName = StringUtils.parseAndCatchGroupAsStringOrNull( HANDLER_PATTERN_STR, getOriginalText(), 1 );
  }
  void parseCollectionName() {
    collectionName = StringUtils.parseAndCatchGroupAsStringOrNull( COLLECTION_PATTERN_STR, getOriginalText(), 1 );
  }
  void parseHits() {
    hits = StringUtils.parseAndCatchGroupAsLongOrNull( HITS_PATTERN_STR, getOriginalText(), 1 );
  }
  void parseStatus() {
    status = StringUtils.parseAndCatchGroupAsLongOrNull( STATUS_PATTERN_STR, getOriginalText(), 1 );
  }
  void parseQTime() {
    qTime = StringUtils.parseAndCatchGroupAsLongOrNull( QTIME_PATTERN_STR, getOriginalText(), 1 );
  }

  // Not thread safe, but OK for now, for single thread utility
  public Map<String,Collection<String>> getParsedSolrParams() {
    if ( null==parsedParamValues ) {
      parsedParamValues = StringUtils.parseCgiParameters( getParamsString() );
    }
    return parsedParamValues;
  }

  public boolean isSolrPattern() {
    return isSolrPattern;
  }
  
  public String getParamsString() {
    return paramsString;
  }

  String getOriginalText() {
    return originalText;
  }

  String getHandlerName() {
    return handlerName;
  }
  String getCollectionName() {
    return collectionName;
  }
  // Don't really need Longs here, but it's what utility returns
  /*
   * get number of Matches
   */
  Long getHits() {
    return hits;
  }
  /*
   * Similar to HTTP Numeric Status Code
   * Eg: 200, 500, etc.
   */
  Long getStatus() {
    return status;
  }
  /*
   * Query time in milliseconds
   * may not include transmission time of payload to requesting client
   */
  Long getQTime() {
    return qTime;
  }

  public static void main(String[] args) throws IOException {
    for ( int i=0; i<args.length; i++ ) {

      // Locate Files
      LogFileRepo repo = new LogFileRepoBase( args[i] );
      Collection<LogFile> logs = repo.findLogFiles();
      for ( LogFile lf : logs ) {
        lf.read();
        Collection<LogEntry> rawEntries = lf.getEntries();
        Collection<LogEntryFromSolr> solrEntries = new ArrayList<>();
        for ( LogEntry rawEntry : rawEntries ) {
          // LogEntryFromSolr solrEntry = new LogEntryFromSolr( rawEntry );
          LogEntry solrEntry = LogEntryFromSolr.solrLogEntryFromBaseEntryOrNull( rawEntry );
          // if ( solrEntry.isSolrPattern() )
          if ( null != solrEntry )
          {
            solrEntries.add( (LogEntryFromSolr) solrEntry );
          }
        }

        // Tabulate
        Map<String,Long> queryTypeCounts = LogEntryFromSolr.tabulateQueryArgCombos( solrEntries );
        // composite-parameter-key -> each-parameter-name-> unique-value -> count
        Map<String,Map<String,Map<String,Long>>> detailedStats = LogEntryFromSolr.tabulateQueryArgCombosAndValues( solrEntries );
        queryTypeCounts = SetUtils.sortMapByValues( queryTypeCounts );
        queryTypeCounts = SetUtils.reverseMapEntryKeyOrder( queryTypeCounts );

        // Report
        for ( Entry<String, Long> e1 : queryTypeCounts.entrySet() ) {
          String queryType = e1.getKey();
          Long queryTypeCount = e1.getValue();
          System.out.println( "" + queryTypeCount + " " + queryType );
          Map<String,Map<String,Long>> statsForQueryType = detailedStats.get( queryType );
          for ( Entry<String, Map<String, Long>> e2 : statsForQueryType.entrySet() ) {
            String paramName = e2.getKey();
            System.out.println( "\t" + paramName + ":" );
            Map<String, Long> paramValues = e2.getValue();
            paramValues = SetUtils.sortMapByValues( paramValues );
            paramValues = SetUtils.reverseMapEntryKeyOrder( paramValues );
            for ( Entry<String, Long> e3 : paramValues.entrySet() ) {
              String value = e3.getKey();
              Long valueCount = e3.getValue();
              System.out.println( "\t\t" + valueCount + " " + value );
            }
          }
        }
      }
      // System.out.println( repo );
    }

  }

}
