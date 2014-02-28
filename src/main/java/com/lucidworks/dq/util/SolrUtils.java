package com.lucidworks.dq.util;

import java.net.URL;
import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class SolrUtils {


  // TODO: get ID field from server, appears in many places
  static String ID_FIELD = "id";
  // TODO: ID also assumed to be a string

  static int ALL_ROWS = Integer.MAX_VALUE;
  
  static String DEFAULT_HOST = "localhost";
  static int DEFAULT_PORT = 8983;

  // Other constant fields at bottom after main()

  // Convenience Factory Methods
  // -------------------------------------------------------------------

  // Construct from:
  // Nothing -> localhost:8983 (no collection, but often collection1)
  // server url as String or URL
  // host + port (as int or String)
  // host + port (int or String) + collection name
  
  public static HttpSolrServer getServer() {
	  return getServer( DEFAULT_HOST, DEFAULT_PORT );
  }
  public static HttpSolrServer getServer( String serverUrl ) {
	  return new HttpSolrServer( serverUrl );
  }
  public static HttpSolrServer getServer( URL serverUrl ) {
	  return getServer( serverUrl.toExternalForm()  );
  }
  public static HttpSolrServer getServer( String host, int port ) {
	  return getServer( host, ""+port );
  }
  public static HttpSolrServer getServer( String host, int port, String collection ) {
	  return getServer( host, ""+port, collection );
  }
  public static HttpSolrServer getServer( String host, String port ) {
	  String url = "http://" + host + ":" + port + "/solr";
	  return getServer( url );
  }
  public static HttpSolrServer getServer( String host, String port, String collection ) {
	  if ( null==host ) {
		host = DEFAULT_HOST;
	  }
	  if ( null==port ) {
		port = "" + DEFAULT_PORT;		  
	  }
	  String url = null;
	  if ( null==collection ) {
        url = "http://" + host + ":" + port + "/solr";
	  }
	  else {
        url = "http://" + host + ":" + port + "/solr/" + collection;		  
	  }
	  return getServer( url );
  }
  
  
  // Basic Queries and Stas
  // -------------------------------

  // Alias
  public static Set<String> getDeclaredFieldNames( HttpSolrServer server ) throws SolrServerException {
	  return getAllSchemaFieldNames( server );
  }
  // Makes multiple calls to server
  public static Set<String> getUnusedDeclaredFieldNames( HttpSolrServer server ) throws SolrServerException {
    Set<String> declaredFields = getDeclaredFieldNames( server );
    Set<String> actualFields = getActualFieldNames( server );
    return SetUtils.inBOnly_destructive( actualFields, declaredFields );
  }
  // Makes multiple calls to server
  public static Set<String> getAllDeclaredAndActualFieldNames( HttpSolrServer server ) throws SolrServerException {
    Set<String> declaredFields = getDeclaredFieldNames( server );
    Set<String> actualFields = getActualFieldNames( server );
    return SetUtils.union_destructive( declaredFields, actualFields );
  }
  // http://localhost:8985/solr/collection1/admin/luke
  public static Set<String> getActualFieldNames( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/admin/luke");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap fields = (SimpleOrderedMap) res2.get("fields");
    for ( int i=0; i<fields.size(); i++ ) {
      String name = fields.getName( i );
      out.add( name );
    }
    // System.out.println( "Luke Fields = " + fields );
//    for ( Object field : fields ) {
//       System.out.println( "Field = " + field );
//    }
    // out.addAll( fields.iterator())
	return out;
  }
  public static Set<String> getLukeFieldsWithStoredValues( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/admin/luke");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap fields = (SimpleOrderedMap) res2.get("fields");
    for ( int i=0; i<fields.size(); i++ ) {
      String name = fields.getName( i );
      SimpleOrderedMap val = (SimpleOrderedMap) fields.getVal( i );
      // String type = (String) val.get( "type" );
      String schemaFlags = (String) val.get( "schema" );
      String indexFlags = (String) val.get( "index" );
      // Integer numDocs = (Integer) val.get( "docs" );
      // Look at 3rd character, offset 2
      if ( null!=schemaFlags && schemaFlags.length() >= 3 ) {
    	String flag = schemaFlags.substring( 2, 3 );
    	if ( flag.equalsIgnoreCase("S") ) {
    	   out.add( name );    		
    	}
      }
    }
	return out;
  }
  // field name -> type name
  public static Map<String,String> getLukeFieldTypes( HttpSolrServer server ) throws SolrServerException {
	Map<String,String> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/admin/luke");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap fields = (SimpleOrderedMap) res2.get("fields");
    for ( int i=0; i<fields.size(); i++ ) {
      String name = fields.getName( i );
      SimpleOrderedMap val = (SimpleOrderedMap) fields.getVal( i );
      String type = (String) val.get( "type" );
      out.put( name, type );
    }
	return out;
  }
  public static Set<String> getLukeFieldsWithIndexedValues( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/admin/luke");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap fields = (SimpleOrderedMap) res2.get("fields");
    for ( int i=0; i<fields.size(); i++ ) {
      String name = fields.getName( i );
      SimpleOrderedMap val = (SimpleOrderedMap) fields.getVal( i );
      // String type = (String) val.get( "type" );
      String schemaFlags = (String) val.get( "schema" );
      String indexFlags = (String) val.get( "index" );
      // Look at 1st character, offset 0
      if ( null!=schemaFlags && schemaFlags.length() >= 1 ) {
    	String flag = schemaFlags.substring( 0, 1 );
    	if ( flag.equalsIgnoreCase("I") ) {
    	   out.add( name );    		
    	}
      }
    }
	return out;
  }
  public static Set<String> _getActualFieldNames( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
    // Look at the first real document
    SolrQuery query = new SolrQuery( "*:*" );
    query.addField( "*" );
    query.setRows( 1 );
    QueryResponse res = server.query( query );
	SolrDocumentList docs = res.getResults();
	if ( ! docs.isEmpty() ) {
	  SolrDocument firstDoc = docs.get(0);
	  out.addAll( firstDoc.getFieldNames() );
	}
	return out;
  }
  // Alias
  public static Set<String> getDeclaredDynamicFieldPatterns( HttpSolrServer server ) throws SolrServerException {
    return getAllDynamicFieldPatterns( server );
  }
  // Makes multiple calls to server
  public static Set<String> getActualDynamicFieldNames( HttpSolrServer server ) throws SolrServerException {
    Set<String> declaredFields = getDeclaredFieldNames( server );
    Set<String> actualFields = getActualFieldNames( server );
    return SetUtils.inAOnly_destructive( actualFields, declaredFields );
  }

  // TODO: lookup actual ID field via getUniqueKeyFieldName / getIdFieldName
  public static Set<String> getAllIds( HttpSolrServer server ) throws SolrServerException {
	  Set<String> out = new LinkedHashSet<>();
	  SolrQuery q = new SolrQuery( "*:*" );
	  // TODO: use proper ID
	  q.addField( ID_FIELD );
	  q.setRows( ALL_ROWS );
	  QueryResponse res = server.query( q );
	  for ( SolrDocument doc : res.getResults() ) {
		  String id = (String) doc.get( ID_FIELD );
		  out.add( id );
	  }
	  return out;
  }
  public static Map<Object,Long> getAllStoredValuesAndCountsForField_ViaNormalQuery( HttpSolrServer server, String fieldName ) throws SolrServerException {
	Map<Object,Long> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery( "*:*" );
	q.addField( fieldName );
	q.setRows( ALL_ROWS );
	QueryResponse res = server.query( q );
	for ( SolrDocument doc : res.getResults() ) {
	  Object value = doc.get( fieldName );
	  Long oldCount = 0L;
	  if ( out.containsKey(value) ) {
		oldCount = out.get(value);
	  }
	  out.put( value, oldCount + 1L );
	}
	return out;
  }
  public static Map<Object,Long> getAllStoredValuesAndCountsForField_ViaGroupedQuery( HttpSolrServer server, String fieldName ) throws SolrServerException {
	Map<Object,Long> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery( "*:*" );
	q.addField( fieldName );
	q.set( "group", true );
	q.set( "group.field", fieldName );
	q.set( "group.limit", 0 );  // No docs, we just want counts
	// q.addField( ID_FIELD );  // moot if not getting docs
	q.setRows( ALL_ROWS );   // All *Groups*
	QueryResponse res = server.query( q );
	GroupResponse groups = res.getGroupResponse();
	List<GroupCommand> grpVals = groups.getValues();
	for ( GroupCommand cmd : grpVals ) {
	  String name = cmd.getName();
	  int numTotalDocMatches = cmd.getMatches();
	  List<Group> cmdVals = cmd.getValues();
	  for ( Group gVal : cmdVals ) {
		String val = gVal.getGroupValue();
		SolrDocumentList grpRes = gVal.getResult();
		long valCount = grpRes.getNumFound();
		out.put( val, valCount );
	  }
	}  
	return out;
  }

  public static long getTotalDocCount( HttpSolrServer server ) throws SolrServerException {
	  return getDocCountForQuery( server, "*:*" );
  }
  public static long getDocCountForField( HttpSolrServer server, String fieldName ) throws SolrServerException {
	// NullPointerException for location
	// com.spatial4j.core.io.ParseUtils.parsePoint(ParseUtils.java:42)
	String queryStr = fieldName + ":[* TO *]";
	try {
	  return getDocCountForQuery( server, queryStr );
	}
	catch( Exception e ) {
	  // TODO: will this wildcard expand to all terms?
	  queryStr = fieldName + ":*";
	  return getDocCountForQuery( server, queryStr );		  
	}
  }
  public static long getStoredDocCountForField( HttpSolrServer server, String fieldName ) throws SolrServerException {
	long out = 0L;
	String queryStr = "*:*";
	SolrQuery q = new SolrQuery( queryStr );
	q.addField( fieldName );
	// q.setRows( ALL_ROWS );
	q.setRows( 1000 );
	QueryResponse res = server.query( q );
	for ( SolrDocument doc : res.getResults() ) {
	  Object value = doc.get( fieldName );
	  // TODO: could check for data types and do specific checks
	  // Eg: if string, do a trim, see if length > 0
	  if ( null!=value ) {
		out++;
	  }
	}
	return out;
  }
  public static Set<String> getEmptyFieldDocIds( HttpSolrServer server, String fieldName ) throws SolrServerException {
	  // NullPointerException for location
	  // com.spatial4j.core.io.ParseUtils.parsePoint(ParseUtils.java:42)
	  String queryStr = "-" + fieldName + ":[* TO *]";
	  Set<String> out = new LinkedHashSet<>();
	  SolrQuery q = new SolrQuery( queryStr );
	  q.addField( ID_FIELD );
	  q.setRows( ALL_ROWS );
	  QueryResponse res = server.query( q );
	  for ( SolrDocument doc : res.getResults() ) {
		  String id = (String) doc.get( ID_FIELD );
		  out.add( id );
	  }
	  return out;
//	  try {
//		  return getDocCountForQuery( server, queryStr );
//	  }
//	  catch( Exception e ) {
//		  // TODO: will this wildcard expand to all terms?
//		  queryStr = fieldName + ":*";
//		  return getDocCountForQuery( server, queryStr );		  
//	  }
  }
  public static long getDocCountForQuery( HttpSolrServer server, String query ) throws SolrServerException {
	  SolrQuery q = new SolrQuery( query );
	  return getDocCountForQuery( server, q );
  }
  // TODO: lookup actual ID field via getUniqueKeyFieldName / getIdFieldName
  public static long getDocCountForQuery( HttpSolrServer server, SolrQuery query ) throws SolrServerException {
	  query.addField( ID_FIELD );    // Minimize data
	  query.setRows( 0 );            // Minimize data
	  QueryResponse res = server.query( query );
	  SolrDocumentList docs = res.getResults();
	  long count = docs.getNumFound();
	  return count;
  }

  // http://localhost:8985/solr/collection1/terms
  // TODO: not returning term counts for now, not really what we're looking at
  public static Set<String> getAllTermsForField_ViaTermsRequest( HttpSolrServer server, String fieldName ) throws SolrServerException {
	return getTermsForField_ViaTermsRequest( server, fieldName, -1 );
  }
  // Get multiple fields at once
  public static Map<String,Map<String,Long>> getAllTermsForFields_ViaTermsRequest( HttpSolrServer server, Set<String> fieldNames ) throws SolrServerException {
	return getTermsForFields_ViaTermsRequest( server, fieldNames, -1 );
  }
  // By default we'll get the top 10
  public static Set<String> getTermsForField_ViaTermsRequest( HttpSolrServer server, String fieldName ) throws SolrServerException {
	return getTermsForField_ViaTermsRequest( server, fieldName, null );
  }
  public static Set<String> getTermsForField_ViaTermsRequest( HttpSolrServer server, String fieldName, Integer optLimit ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/terms");
	q.addTermsField( fieldName );
	if ( null!=optLimit ) {
		q.setTermsLimit( optLimit );
	}
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap res3 = (SimpleOrderedMap) res2.get("terms");
    NamedList terms = (NamedList) res3.get( fieldName );
    for ( int i=0; i<terms.size(); i++ ) {
      String name = terms.getName( i );
      out.add( name );
    }
	return out;
  }

  // By default gets the top 10
  public static Map<String,Long> getAllTermsAndCountsForField_ViaTermsRequest( HttpSolrServer server, String fieldName ) throws SolrServerException {
	return getTermsAndCountsForField_ViaTermsRequest( server, fieldName, -1 );
  }
  public static Map<String,Long> getTermsAndCountsForField_ViaTermsRequest( HttpSolrServer server, String fieldName ) throws SolrServerException {
	return getTermsAndCountsForField_ViaTermsRequest( server, fieldName, null );
  }
  // Includes deleted Docs
  public static Map<String,Long> getTermsAndCountsForField_ViaTermsRequest( HttpSolrServer server, String fieldName, Integer optLimit ) throws SolrServerException {
	Map<String,Long> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/terms");
    q.addTermsField( fieldName );
	if ( null!=optLimit ) {
	  q.setTermsLimit( optLimit );
	}
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap res3 = (SimpleOrderedMap) res2.get("terms");
    NamedList terms = (NamedList) res3.get( fieldName );
	for ( int i=0; i<terms.size(); i++ ) {
	  String name = terms.getName( i );
	  Integer count = (Integer) terms.getVal( i );
	  out.put( name, new Long(count) );
	}
    return out;
  }


  // By default gets the top 10
  public static Map<String,Map<String,Long>> getTermsForFields_ViaTermsRequest( HttpSolrServer server, Set<String> fieldNames ) throws SolrServerException {
	return getTermsForFields_ViaTermsRequest( server, fieldNames, null );
  }
  // Includes deleted Docs
  public static Map<String,Map<String,Long>> getTermsForFields_ViaTermsRequest( HttpSolrServer server, Set<String> fieldNames, Integer optLimit ) throws SolrServerException {
	Map<String,Map<String,Long>> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/terms");
	for ( String fieldName : fieldNames ) {
	  q.addTermsField( fieldName );
	}
	if ( null!=optLimit ) {
	  q.setTermsLimit( optLimit );
	}
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    SimpleOrderedMap res3 = (SimpleOrderedMap) res2.get("terms");
	for ( String fieldName : fieldNames ) {
      NamedList terms = (NamedList) res3.get( fieldName );
      Map<String,Long> termVector = new LinkedHashMap<>();
	  for ( int i=0; i<terms.size(); i++ ) {
	    String name = terms.getName( i );
	    Integer count = (Integer) terms.getVal( i );
	    termVector.put( name, new Long(count) );
	  }
      out.put( fieldName, termVector );
	}
    return out;
  }

  // Returns only active documents
  // BUT slow for large sets
  // http://localhost:8983/solr/collection1/select?q=*:*&rows=0&facet=true&fl=id&facet.limit=-1&facet.field=class&facet.field=type&rows=0
  public static Map<String,Map<String,Long>> getTermsForFields_ViaSearchFacets( HttpSolrServer server, Set<String> fieldNames, Integer optLimit ) throws SolrServerException {
	Map<String,Map<String,Long>> out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery( "*:*" );
	q.addField( ID_FIELD );    // Minimize data
	q.setRows( 0 );            // Minimize data
	for ( String fieldName : fieldNames ) {
	  q.addFacetField( fieldName );
	}
	if ( null!=optLimit ) {
	  q.setFacetLimit( optLimit );
	}
	QueryResponse res = server.query( q );
	List<FacetField> facets = res.getFacetFields();
	// Foreach Field
	for ( FacetField facet : facets ) {
      Map<String,Long> termVector = new LinkedHashMap<>();
      String fieldName = facet.getName();
      Integer facetValuesCount = (Integer) facet.getValueCount();
      List<Count> vals = facet.getValues();
      System.out.println( fieldName + " has " + facetValuesCount + " entries" );
	  for ( Count val : facet.getValues() ) {
		String term = val.getName();
		// seems to always return 0 ?
		long termCount = val.getCount();
		termVector.put( term, new Long(termCount) );
		// FacetField ffield = val.getFacetField();
		// Class<? extends Count> fclass = val.getClass();
		// String filter = val.getAsFilterQuery();
	  }
	  out.put( fieldName, termVector );
	}
	return out;
  }

  public static Map< String, Map<String, Collection<Object>> > getAllStoredValuesForFields_ByDocument( HttpSolrServer server, Set<String> fieldNames ) throws SolrServerException {
	return getStoredValuesForFields_ByDocument( server, fieldNames, ALL_ROWS );
  }
  // returns Map: docId -> fieldName -> values
  // Mirrors SolrJ structure
  public static Map< String, Map<String,Collection<Object>> > getStoredValuesForFields_ByDocument( HttpSolrServer server, Set<String> fieldNames, Integer optLimit  ) throws SolrServerException {
	Map< String, Map<String, Collection<Object>> > out = new LinkedHashMap<>();
	SolrQuery q = new SolrQuery( "*:*" );
	boolean forcedId = false;
	boolean sawWildcard = false;
	if ( null!=fieldNames && ! fieldNames.isEmpty() ) {
	  boolean haveSeenId = false;
	  for ( String fieldName : fieldNames ) {
        q.addField( fieldName );
        if ( fieldName.equals("*") ) {
          sawWildcard = true;
          haveSeenId = true;
        }
        else if ( fieldName.equals(ID_FIELD) ) {
          haveSeenId = true;
        }
	  }
	  if ( ! haveSeenId ) {
		// TODO: lookup real ID field
	    q.addField( ID_FIELD );
		forcedId = true;
	  }
	}
	else {
	  q.addField( "*" );	
	}
	if ( null!=optLimit ) {
	  q.setRows( optLimit );
	}
	QueryResponse res = server.query( q );
	for ( SolrDocument doc : res.getResults() ) {
	  // TODO: lookup real ID field
	  String id = doc.getFirstValue( ID_FIELD ).toString();
	  // doc.getFieldNames();
	  Map<String, Collection<Object>> values = doc.getFieldValuesMap();
	  // If they really didn't want the ID, then remove it
	  if ( forcedId ) {
		values.remove( ID_FIELD );
	  }
	  out.put( id, values );
	}
	return out;
  }
  public static Map< String, Map<String,Collection<Object>> > getAllStoredValuesForFields_ByField( HttpSolrServer server, Set<String> fieldNames ) throws SolrServerException {
	return getStoredValuesForFields_ByField( server, fieldNames, ALL_ROWS );
  }
  public static Map< String, Map<String,Collection<Object>> > getStoredValuesForFields_ByField( HttpSolrServer server, Set<String> fieldNames, Integer optLimit ) throws SolrServerException {
	return transformStoredValuesData_ByDocument2ByField(
		       getStoredValuesForFields_ByDocument( server, fieldNames, optLimit )
		   );
  }
  // Input Map: docId -> fieldName -> values
  // returns Map: fieldName -> docId -> values
  static Map< String, Map<String,Collection<Object>> > transformStoredValuesData_ByDocument2ByField( Map< String, Map<String,Collection<Object>> > byDocValues ) {
	Map< String, Map<String,Collection<Object>> > out = new LinkedHashMap<>();
    // Foreach Document
    for ( Entry<String, Map<String, Collection<Object>>> docEntry : byDocValues.entrySet() ) {
  	  String docId = docEntry.getKey();
	  Map<String, Collection<Object>> docValuesRaw = docEntry.getValue();
	  // java.lang.UnsupportedOperationException
	  // Map<String, Collection<Object>> docValues = new LinkedHashMap<>();
	  // java.lang.UnsupportedOperationException
	  // docValues.putAll( docValuesRaw );
	  // for ( Entry<String, Collection<Object>> fieldEntry : docValues.entrySet() )
	  for ( String fieldName : docValuesRaw.keySet() )
	  {
		// String fieldName = fieldEntry.getKey();
		// Collection<Object> values = fieldEntry.getValue();
		Collection<Object> values = docValuesRaw.get( fieldName );
		Map<String,Collection<Object>> aggregatedFieldVector = null;
		if ( out.containsKey(fieldName) ) {
		  aggregatedFieldVector = out.get( fieldName );
		}
		else {
		  aggregatedFieldVector = new LinkedHashMap<>();
		  out.put( fieldName, aggregatedFieldVector );
		}
		aggregatedFieldVector.put( docId, values );
	  }
    }
    return out;
  }

  // Uses FieldName-first layered Map
  // Input: fieldName -> docId -> values
  // Returns Map: fieldName -> fieldValue -> total values count
  // You typically pass in input obtained from calling transformStoredValuesData_ByDocument2ByField
  // Do NOT pass in data directly from getStoredValuesForFields_ByDocument, it's in the wrong format
  // even though the generic signatures might match.
  public static Map< String, Map<String,Long> > flattenStoredValues_ValueToTotalCount( Map< String, Map<String,Collection<Object>> > storedValues_ByField ) {
	Map< String, Map<String,Long> > out = new LinkedHashMap<>();
	// Foreach field
	for ( Entry<String, Map<String, Collection<Object>>> fieldEntry : storedValues_ByField.entrySet() ) {
	  String fieldName = fieldEntry.getKey();
	  Map<String,Collection<Object>> data = fieldEntry.getValue();
	  // Map: valueStr -> count
	  Map<String,Long> valueCounts = new LinkedHashMap<>();
	  // Foreach doc
	  for ( Entry<String, Collection<Object>> docEntry : data.entrySet() ) {
		String docId = docEntry.getKey();
		Collection<Object> values = docEntry.getValue();
		for ( Object v : values ) {
		  String valKey = v.toString();
		  if ( valueCounts.containsKey(valKey) ) {
			Long prevCount = valueCounts.get( valKey );
			valueCounts.put( valKey, prevCount + 1L );
		  }
		  else {
			  valueCounts.put( valKey, 1L );
		  }
		}
	  }  // End foreach doc
	  out.put( fieldName, valueCounts );
	}  // End foreach field
	return out;
  }
  // Uses FieldName-first layered Map
  // Input: fieldName -> docId -> values
  // Returns Map: fieldName -> document count
  public static Map<String, Long> flattenStoredValues_ToDocCount( Map< String, Map<String,Collection<Object>> > storedValues_ByField ) {
	Map<String, Long> out = new LinkedHashMap<>();
	// Intermediate Map: fieldName -> fieldValue -> docIds
	Map< String, Map<String,Set<String>> > intermediateData = flattenStoredValues_ValueToDocIds( storedValues_ByField );
	// Foreach Field
	for ( Entry<String, Map<String, Set<String>>> fieldEntry : intermediateData.entrySet() ) {
	  String fieldName = fieldEntry.getKey();
	  Map<String, Set<String>> values = fieldEntry.getValue();
	  // Tabulate Doc IDs across all values for this field
	  Set<String> overallIds = new LinkedHashSet<>();
	  for ( Entry<String, Set<String>> valueEntry : values.entrySet() ) {
		String valueStr = valueEntry.getKey();
		Set<String> docIds = valueEntry.getValue();
		overallIds.addAll( docIds );
	  }
	  out.put( fieldName, new Long(overallIds.size()) );
	}
	return out;
  }
  // Uses FieldName-first layered Map
  // Input: fieldName -> docId -> values
  // Returns Map: fieldName -> fieldValue -> docIds
  // You typically pass in input obtained from calling transformStoredValuesData_ByDocument2ByField
  // Do NOT pass in data directly from getStoredValuesForFields_ByDocument, it's in the wrong format
  // even though the generic signatures might match.
  public static Map< String, Map<String,Set<String>> > flattenStoredValues_ValueToDocIds( Map< String, Map<String,Collection<Object>> > storedValues_ByField ) {
	Map< String, Map<String,Set<String>> > out = new LinkedHashMap<>();
	// Foreach field
	for ( Entry<String, Map<String, Collection<Object>>> fieldEntry : storedValues_ByField.entrySet() ) {
	  String fieldName = fieldEntry.getKey();
	  Map<String,Collection<Object>> data = fieldEntry.getValue();
	  // Map: valueStr -> set<docIds>
	  Map<String,Set<String>> valueToDocIds = new LinkedHashMap<>();
	  // Foreach doc
	  for ( Entry<String, Collection<Object>> docEntry : data.entrySet() ) {
		String docId = docEntry.getKey();
		Collection<Object> values = docEntry.getValue();
		for ( Object v : values ) {
		  String valKey = v.toString();
		  Set<String> docIdsForValue = null;
		  if ( valueToDocIds.containsKey(valKey) ) {
			docIdsForValue = valueToDocIds.get(valKey);
		  }
		  else {
			docIdsForValue = new LinkedHashSet<>();
			valueToDocIds.put( valKey, docIdsForValue );
		  }
		  docIdsForValue.add( docId );
		}
	  }  // End foreach doc
	  out.put( fieldName, valueToDocIds );
	}  // End foreach field
	return out;
  }

  
  // Info From REST API Calls
  // -------------------------------------------------------------------------------

  // https://cwiki.apache.org/confluence/display/solr/Schema+API
  // TODO: These are also in SchemaFromRestAdHock, should they also be here?
  // - Maybe handy to still have them here for quick lookup (vs. deailed reports)
  // - Ad-Hock can get some info that fancier shema class can't (at this time)
  //   Eg: getSimilarityModelClassName, getDefaultOperator

  public static float getSchemaVersion( HttpSolrServer server ) throws SolrServerException {
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/version"); 
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      float version = (float) res2.get("version");
      // float version = (float) res.getResponse().get("version");
      return version;
  }
  public static String getSchemaName( HttpSolrServer server ) throws SolrServerException {
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/name"); 
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      String name = (String) res2.get("name");
      // float version = (float) res.getResponse().get("version");
      return name;
  }
  // Alias
  // Common Name
  public static String getIdFieldName( HttpSolrServer server ) throws SolrServerException {
	  return getUniqueKeyFieldName( server );
  }
  // REST Name
  public static String getUniqueKeyFieldName( HttpSolrServer server ) throws SolrServerException {
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/uniquekey");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      String key = (String) res2.get("uniqueKey");
      // float version = (float) res.getResponse().get("version");
      return key;
  }
  public static String getSimilarityModelClassName( HttpSolrServer server ) throws SolrServerException {
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/similarity");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      NamedList<Object> sim = (NamedList<Object>) res2.get("similarity");
      String className = (String) sim.get("class");
      // float version = (float) res.getResponse().get("version");
      // return sim;
      return className;
  }
  public static String getDefaultOperator( HttpSolrServer server ) throws SolrServerException {
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/solrqueryparser/defaultoperator");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      String op = (String) res2.get("defaultOperator");
      // float version = (float) res.getResponse().get("version");
      return op;
  }
 
  public static Set<String> getAllSchemaFieldNames( HttpSolrServer server ) throws SolrServerException {
	  Set<String> out = new LinkedHashSet<>();
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/fields");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("fields");
      //System.out.println( "fields=" + fields );
      for ( SimpleOrderedMap f : fields ) {
    	  //System.out.println( "f=" + f );
    	  String name = (String)f.get( "name" );
    	  out.add( name );
      }
      return out;
  }
  public static Set<String> getAllDynamicFieldPatterns( HttpSolrServer server ) throws SolrServerException {
	  Set<String> out = new LinkedHashSet<>();
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/dynamicfields");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("dynamicFields");
      // System.out.println( "fields=" + fields );
      for ( SimpleOrderedMap f : fields ) {
    	  // System.out.println( "f=" + f );
    	  String name = (String)f.get( "name" );
    	  out.add( name );
      }
      return out;
  }
  public static Set<String> getAllFieldTypeNames( HttpSolrServer server ) throws SolrServerException {
	  Set<String> out = new LinkedHashSet<>();
	  SolrQuery q = new SolrQuery();
	  q.setRequestHandler("/schema/fieldtypes");
      QueryResponse res = server.query( q );
      NamedList<Object> res2 = res.getResponse();
      Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("fieldTypes");
      // System.out.println( "fields=" + fields );
      for ( SimpleOrderedMap f : fields ) {
    	  // System.out.println( "f=" + f );
    	  String name = (String)f.get( "name" );
    	  out.add( name );
      }
      return out;
  }

  public static Set<String> getAllCopyFieldSourceNames( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/schema/copyfields");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "source" );
      out.add( name );
    }
    return out;
  }
  public static Set<String> getAllCopyFieldDestinationNames( HttpSolrServer server ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/schema/copyfields");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "dest" );
      out.add( name );
    }
    return out;
  }
  public static Set<String> getCopyFieldDestinationsForSource( HttpSolrServer server, String sourceName ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/schema/copyfields");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String source = (String)f.get( "source" );
      String dest = (String)f.get( "dest" );
      if ( source.equals(sourceName) ) {
    	  out.add( dest );
      }
    }
    return out;
  }
  public static Set<String> getCopyFieldSourcesForDestination( HttpSolrServer server, String sourceName ) throws SolrServerException {
	Set<String> out = new LinkedHashSet<>();
	SolrQuery q = new SolrQuery();
	q.setRequestHandler("/schema/copyfields");
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>)res2.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String source = (String)f.get( "source" );
      String dest = (String)f.get( "dest" );
      if ( dest.equals(sourceName) ) {
    	  out.add( source );
      }
    }
    return out;
  }

  // http://localhost:8983/solr/demo_shard1_replica1/select?q=*:*&stats=true&stats.field=releaseDate&stats.field=startDate&rows=0
  public static FieldStatsInfo getStatsForField( HttpSolrServer server, String fieldName ) throws SolrServerException {
	SolrQuery q = new SolrQuery( "*:*" );
	q.set( "stats", true );
	q.set( "stats.field", fieldName );
	q.setRows( 0 );
	QueryResponse res = server.query( q );
    Map<String, FieldStatsInfo> fieldStats = res.getFieldStatsInfo();
    return fieldStats.get( fieldName );
//    for ( Entry<String, FieldStatsInfo> entry : fieldStats.entrySet() ) {
//      String fieldName2 = entry.getKey();
//      FieldStatsInfo vals = entry.getValue();
//      String fieldName3 = vals.getName();
//      Object min = vals.getMin();
//      Object max = vals.getMax();
//      Object sum = vals.getSum();
//      Object mean = vals.getMean();
//      Double std = vals.getStddev();
//      Long count = vals.getCount();
//      Long missing = vals.getMissing();
//      System.out.println( "Stats=" + vals );
//    }
  }

  public static Map<java.util.Date,Long> getHistogramForDateField( HttpSolrServer server, String fieldName, int gapInYears ) throws SolrServerException, ParseException {
	FieldStatsInfo stats = getStatsForField( server, fieldName );
	java.util.Date startObj = (java.util.Date) stats.getMin();
	java.util.Date endObj = (java.util.Date) stats.getMax();
	System.out.println( "Stats, Objs: Start/Stop: " + startObj + " / " + endObj );
	String start = DateUtils.date2SolrXmlZulu_date2str( startObj );
	String end = DateUtils.date2SolrXmlZulu_date2str( endObj );
	System.out.println( "Stats, Strs: Start/Stop: " + start + " / " + end );

    // http://lucene.apache.org/solr/4_0_0/solr-core/org/apache/solr/util/DateMathParser.html
	String gap = "+" + gapInYears + "YEARS";  // Eg: "+5YEARS"

	// return getHistogramForDateField( server, fieldName, start.toString(), end.toString(), gap );
	// Mon Jul 10 17:00:00 PDT 2006
	// 2012-07-29T00:00:00Z
	// return getHistogramForDateField( server, fieldName, "2006-07-10T17:00:00Z", end.toString(), gap ); 
	return getHistogramForDateField( server, fieldName, start, end, gap );
  }

  // Input needs to be strings so we can also use Solr date math
  // https://lucene.apache.org/solr/4_0_0/solr-solrj/org/apache/solr/client/solrj/response/RangeFacet.html
  // http://localhost:8983/solr/demo_shard1_replica1/select?q=*:*&rows=0&facet=true&facet.range=releaseDate&facet.range=startDate&facet.range.start=NOW-30YEARS&facet.range.end=NOW&facet.range.gap=%2B5YEARS
  public static Map<java.util.Date,Long> getHistogramForDateField( HttpSolrServer server, String fieldName, String start, String end, String gap ) throws SolrServerException, ParseException {
	Map<java.util.Date,Long> out = new LinkedHashMap<>();
    SolrQuery q = new SolrQuery( "*:*" );
    // q.addField( ID_FIELD );    // Minimize data
    q.setRows( 0 );            // Minimize data
    q.setFacetLimit( -1 );     // all values
	q.set( "facet", true );
	q.set( "facet.range", fieldName );
	q.set( "facet.range.start", start );
	q.set( "facet.range.end", end );
	q.set( "facet.range.gap", gap );
	QueryResponse res = server.query( q );
    // List<FacetField> facets = res.getFacetFields();
	List<RangeFacet> facets = res.getFacetRanges();
	// Foreach Facet
	// for ( FacetField f : facets )
	for ( RangeFacet f : facets )
	{
	  System.out.println( "Facet: " + f );
	  List counts = f.getCounts();
	  for ( Object c : counts ) {
		RangeFacet.Count c2 = (RangeFacet.Count) c;
		int countI = c2.getCount();
		String val = c2.getValue();
		// System.out.println( "\tval / count: " + val + " / " + countI );
		java.util.Date valDate = DateUtils.solrXmlZulu2Date_str2date( val );
		out.put( valDate, new Long(countI) );
	  }
	  // System.out.println( "Facet: " + f + ", counts=" + counts );
	}
	return out;
  }


  public static void main( String[] argv ) throws SolrServerException, ParseException {
	String host = "localhost";
	int port = 8983;
	String coll = "demo_shard1_replica1";
	// int port = 8985;
	// String coll = "collection1";
	HttpSolrServer s = getServer( host, port, coll );
	
	// getStatsForField( s, "releaseDate" );
	// getHistogramForDateField( s, "startDate", "NOW-30YEARS", "NOW", "+5YEARS" );
	Map<java.util.Date,Long> histo = getHistogramForDateField( s, "startDate", 5 );
	System.out.println( "Histogram: " + histo );
	for ( Entry<java.util.Date, Long> entry : histo.entrySet() ) {
		java.util.Date dateRaw = entry.getKey();
	  // String dateFmt = DateUtils.javaDefault2SolrXmlZulu_str2str( dateRaw );
	  // String dateFmt = DateUtils.solrXmlZulu2JavaDefault_str2str( dateRaw );
	  Long count = entry.getValue();
	  System.out.println( "\t" + dateRaw + ": " + count );
	  // System.out.println( "\t" + dateFmt + ": " + count );
	}

//	Map<String,String> fieldTypes = getLukeFieldTypes(s);
//	System.out.println( "Field -> Type:" );
//	for ( Entry<String, String> entry : fieldTypes.entrySet() ) {
//	  String fieldName = entry.getKey();
//	  String typeName = entry.getValue();
//	  System.out.println( "\t" + fieldName + ": " + typeName );
//	}

//	Set<String> storedFields = getLukeFieldsWithStoredValues( s );
//	System.out.println( "storedFields = " + storedFields );
//
//	Set<String> indexedFields = getLukeFieldsWithIndexedValues( s );
//	System.out.println( "indexedFields = " + storedFields );
//	
//	Set<String> storedButNotIndexed = SetUtils.inAOnly_nonDestructive( storedFields, indexedFields );
//	Set<String> indexedButNotStored = SetUtils.inBOnly_nonDestructive( storedFields, indexedFields );
//	System.out.println( "storedButNotIndexed = " + storedButNotIndexed );
//	System.out.println( "indexedButNotStored = " + indexedButNotStored );
//	
//	Set<String> allFields = getAllDeclaredAndActualFieldNames(s);
//	Set<String> indexedAndOrStored = SetUtils.union_nonDestructive( storedFields, indexedFields );
//	Set<String> neitherIndexedNorStored = SetUtils.inAOnly_nonDestructive( allFields, indexedAndOrStored );
//	System.out.println( "Sanity Check: neitherIndexedNorStored = " + neitherIndexedNorStored );

	// String fieldName = "name";
	// String fieldName = "color";
	// color, condition, department, format, genre, manufacturer, mpaaRating
	// class, subclass, studio, softwareGrade, mpaaRating, albumLabel
	// categoryIds, categoryNames
	// Set<String> terms = getAllTermsForField_ViaTermsRequest( s, fieldName );
	// System.out.println( "Field " + fieldName + " has " + terms.size() + " terms" );
//    System.out.println( "Terms for field " + fieldName + " = " + terms );
//	Map<String,Set<String>> terms = getTermsForFields( s, getActualFieldNames(s) );
//    System.out.println( "Terms = " + terms );

//	Set<String> declFields = getDeclaredFieldNames( s );
//    System.out.println( "Declared Fields = " + declFields );
//	Set<String> patterns = getDeclaredDynamicFieldPatterns( s );
//    System.out.println( "Dynamic Patterns = " + patterns );
//    Set<String> dynFields = getActualDynamicFieldNames( s );
//    System.out.println( "Dynamic fields = " + dynFields );
//
//    // Experiment
//    Set<String> declaredFields = getDeclaredFieldNames( s );
//    Set<String> actualFields = getActualFieldNames( s );
//    Set<String> schemaOnlyFields = SetUtils.inBOnly_destructive( actualFields, declaredFields );
//    System.out.println( "Experiment: Schema-Only fields = " + schemaOnlyFields );

//	HttpSolrServer s1 = new HttpSolrServer( URL1 );
//    HttpSolrServer s2 = new HttpSolrServer( URL2 );
//
//    float versA = getSchemaVersion( s1 );
//    // float versB = getSchemaVersion( s2 );
//    System.out.println( "Schema Version A: " + versA );
//    String nameA = getSchemaName( s1 );
//    System.out.println( "Schema Name A: " + nameA );
//    String keyA = getUniqueKeyFieldName( s1 );
//    System.out.println( "Key Field A: " + keyA );
//    String defOpA = getDefaultOperator( s1 );
//    System.out.println( "Default Operator A: " + defOpA );
//    String simA = getSimilarityModelClassName( s1 );
//    System.out.println( "Similarity Class Name A: " + simA + ", is-a " + simA.getClass().getName() );
//    
//    // getAllSchemaFieldNames
//    Set<String> fieldsA = getAllSchemaFieldNames( s1 );
//    // Set<String> fieldsB = getAllSchemaFieldNames( s2 );
//    System.out.println( "Fields A: " + fieldsA );
//    // System.out.println( "Feilds B: " + fieldsB );
//
//    Set<String> dynFieldsA = getAllDynamicFieldPatterns( s1 );
//    // Set<String> dynFieldsB = getAllDynamicFieldPatterns( s2 );
//    System.out.println( "Dynamic field Patterns A: " + dynFieldsA );
//    // System.out.println( "Dynamic feild Patterns B: " + dynFieldsB );
//  
//    // getAllFieldTypeNames
//    Set<String> typeNamesA = getAllFieldTypeNames( s1 );
//    // Set<String> typeNamesB = getAllFieldTypeNames( s2 );
//    System.out.println( "Types A: " + typeNamesA );
//    // System.out.println( "Types B: " + typeNamesB );
//    
//    // getAllCopyFieldSourceNames
//    Set<String> sourceNamesA = getAllCopyFieldSourceNames( s1 );
//    // Set<String> sourceNamesB = getAllCopyFieldSourceNames( s2 );
//    System.out.println( "Copy Sources A: " + sourceNamesA );
//    // System.out.println( "Copy Sources B: " + sourceNamesB );
//
//    // getCopyFieldDestinationsForSource
//    for ( String source : sourceNamesA ) {
//    	Set<String> tmpDests = getCopyFieldDestinationsForSource( s1, source );
//    	System.out.println( "\tFrom: '"+ source + "' To " + tmpDests );
//    }
//
//    // getAllCopyFieldDestinationNames
//    Set<String> destNamesA = getAllCopyFieldDestinationNames( s1 );
//    // Set<String> destNamesB = getAllCopyFieldDestinationNames( s2 );
//    System.out.println( "Copy Destinations A: " + destNamesA );
//    // System.out.println( "Copy Destinations B: " + destNamesB );
//
//    // getCopyFieldSourcesForDestination
//    for ( String dest : destNamesA ) {
//    	Set<String> tmpSrcs = getCopyFieldSourcesForDestination( s1, dest );
//    	System.out.println( "\tDest: '"+ dest + "' From " + tmpSrcs );
//    }

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