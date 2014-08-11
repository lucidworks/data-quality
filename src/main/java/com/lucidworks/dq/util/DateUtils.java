package com.lucidworks.dq.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class DateUtils {

  public static final String JAVA_FORMAT = "EEE MMM dd HH:mm:ss z yyyy";
  public static final String ZULU_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  // public static final String COMPACT_LOG_FORMAT = "yyyy-MM-dd_HH:mm:ss.S";
  public static final String COMPACT_LOG_FORMAT = "yyyy-MM-dd_HH:mm:ss.SSS";

  public static String getLocalTimestamp( Date inDate ) {
    DateFormat compactFormatter = new SimpleDateFormat( COMPACT_LOG_FORMAT );
    // NOT setting timezone
    return compactFormatter.format( inDate );
  }
  public static String getLocalTimestamp() {
    return getLocalTimestamp( new Date() );
  }
  public static String getLocalTimestamp( long ms ) {
    return getLocalTimestamp( new Date(ms) );
  }
  public static String javaDefault2SolrXmlZulu_str2str( String inDate ) throws ParseException {
    java.util.Date dateObj = javaDefault2Date_str2date( inDate );
    String outDateStr = date2SolrXmlZulu_date2str( dateObj );
    return outDateStr;
  }
  public static String solrXmlZulu2JavaDefault_str2str( String inDate ) throws ParseException {
    java.util.Date dateObj = solrXmlZulu2Date_str2date( inDate );
    String outDateStr = date2JavaDefault_date2str( dateObj );
    return outDateStr;
  }
  public static String _javaDefault2SolrXmlZulu_str2str( String inDate ) throws ParseException {
    DateFormat javaFormatter = new SimpleDateFormat( JAVA_FORMAT );
    DateFormat zuluFormatter = new SimpleDateFormat( ZULU_FORMAT );
    zuluFormatter.setTimeZone( TimeZone.getTimeZone("GMT") );
    java.util.Date tmpDate = javaFormatter.parse( inDate );
    String outDate = zuluFormatter.format( tmpDate );
    return outDate;
  }
  public static String _solrXmlZulu2JavaDefault_str2str( String inDate ) throws ParseException {
    DateFormat zuluFormatter = new SimpleDateFormat( ZULU_FORMAT );
    zuluFormatter.setTimeZone( TimeZone.getTimeZone("GMT") );
    DateFormat javaFormatter = new SimpleDateFormat( JAVA_FORMAT );
    java.util.Date tmpDate = zuluFormatter.parse( inDate );
    String outDate = javaFormatter.format( tmpDate );
    return outDate;
  }

  public static String date2SolrXmlZulu_date2str( java.util.Date inDate ) throws ParseException {
    DateFormat zuluFormatter = new SimpleDateFormat( ZULU_FORMAT );
    zuluFormatter.setTimeZone( TimeZone.getTimeZone("GMT") );
    String outDate = zuluFormatter.format( inDate );
    return outDate;
  }
  public static String date2JavaDefault_date2str( java.util.Date inDate ) throws ParseException {
    DateFormat javaFormatter = new SimpleDateFormat( JAVA_FORMAT );
    String outDate = javaFormatter.format( inDate );
    return outDate;
  }

  public static java.util.Date javaDefault2Date_str2date( String inDate ) throws ParseException {
    DateFormat javaFormatter = new SimpleDateFormat( JAVA_FORMAT );
    java.util.Date outDate = javaFormatter.parse( inDate );
    return outDate;
  }
  public static java.util.Date solrXmlZulu2Date_str2date( String inDate ) throws ParseException {
    DateFormat zuluFormatter = new SimpleDateFormat( ZULU_FORMAT );
    zuluFormatter.setTimeZone( TimeZone.getTimeZone("GMT") );
    java.util.Date outDate = zuluFormatter.parse( inDate );
    return outDate;
  }

  public static List<Double> dates2Doubles( Collection<Date> dates ) {
    List<Double> out = new ArrayList<>();
    for ( Date d : dates ) {
      out.add(  new Double( d.getTime() )  );
    }
    return out;
  }
  public static Double date2Double( Date d ) {
    return new Double( d.getTime() ).doubleValue();
  }
}