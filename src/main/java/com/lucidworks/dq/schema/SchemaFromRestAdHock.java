package com.lucidworks.dq.schema;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

// Most of this is also in SchemaFromRest
// which does a single request and stores the info
// BUT there are a couple methods that don't work there
// Eg: getSimilarityModelClassName, getDefaultOperator

public class SchemaFromRestAdHock extends SchemaBase implements Schema {

  static String DEFAULT_HOST = "localhost";
  static String DEFAULT_PORT = "8983";
  static String DEFAULT_COLL = "collection1"; // "demo_shard1_replica1";
  static String DEFAULT_URL = "http://" + DEFAULT_HOST + ":" + DEFAULT_PORT + "/solr/" + DEFAULT_COLL;
  // + "/select?q=*:*&rows=" + ROWS + "&fl=id&wt=json&indent=on"

  // TODO: get ID field from server
  static String ID_FIELD = "id";
  // ^--ID also assumed to be a string
  static int ALL_ROWS = Integer.MAX_VALUE;

  static String NOT_AVAILABLE = "(not-available)";

  HttpSolrServer server;

  public SchemaFromRestAdHock() throws MalformedURLException {
    this( DEFAULT_URL );
  }
  public SchemaFromRestAdHock( String host, String port ) {
    this( host, port, DEFAULT_COLL );
  }
  public SchemaFromRestAdHock( String host, int port ) {
    this( host, port, DEFAULT_COLL );
  }
  public SchemaFromRestAdHock( String host, int port, String collection ) {
    this( host, ""+port, collection );
  }
  public SchemaFromRestAdHock( String host, String port, String collection ) {
    String url = "http://" + host + ":" + port + "/solr/" + collection;
    init( url );
  }

  public SchemaFromRestAdHock( URL serverUrl ) {
    init( serverUrl.toExternalForm() );
  }
  public SchemaFromRestAdHock( String serverUrl ) throws MalformedURLException {
    init( serverUrl );
  }
  void init( String serverUrl ) {
    server = new HttpSolrServer( serverUrl );
  }

  // Ad-Hock calls, but use a Solr Server that can be constructed in many ways

  public Set<String> getIds() throws SolrServerException {
    return getIds( server );
  }
  public float getSchemaVersion() throws SolrServerException {
    return getSchemaVersion( server );
  }
  public String getSchemaName() throws SolrServerException {
    return getSchemaName( server );
  }
  public String getUniqueKeyFieldName() throws SolrServerException {
    return getUniqueKeyFieldName( server );
  }
  public String getSimilarityModelClassName() throws SolrServerException {
    return getSimilarityModelClassName( server );
  }
  public String getDefaultOperator() throws SolrServerException {
    return getDefaultOperator( server );
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getDefaultSearchField()
   */
  //@Override
  public String getDefaultSearchField() {
    return NOT_AVAILABLE;
    // TODO: REST Call for this?
  }

  public Set<String> getAllSchemaFieldNames() throws SolrServerException {
    return getAllSchemaFieldNames( server );
  }
  public Set<String> getAllDynamicFieldPatterns() throws SolrServerException {
    return getAllDynamicFieldPatterns( server );
  }
  public Set<String> getAllFieldTypeNames() throws SolrServerException {
    return getAllFieldTypeNames( server );
  }

  public Set<String> getAllCopyFieldSourceNames() throws SolrServerException {
    return getAllCopyFieldSourceNames( server );
  }
  public Set<String> getAllCopyFieldDestinationNames() throws SolrServerException {
    return getAllCopyFieldDestinationNames( server );
  }
  public Set<String> getCopyFieldDestinationsForSource( String sourceName ) throws SolrServerException {
    return getCopyFieldDestinationsForSource( server, sourceName );
  }
  public Set<String> getCopyFieldSourcesForDestination( String destName ) throws SolrServerException {
    return getCopyFieldSourcesForDestination( server, destName );
  }

  // Static Ad-Hock Calls

  public static Set<String> getIds( HttpSolrServer server ) throws SolrServerException {
    Set<String> out = new LinkedHashSet<>();
    SolrQuery q = new SolrQuery( "*:*" );
    q.addField( ID_FIELD );
    q.setRows( ALL_ROWS );
    QueryResponse res = server.query( q );
    for ( SolrDocument doc : res.getResults() ) {
      String id = (String) doc.get( ID_FIELD );
      out.add( id );
    }
    return out;
  }

  // https://cwiki.apache.org/confluence/display/solr/Schema+API

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
  public static Set<String> getCopyFieldSourcesForDestination( HttpSolrServer server, String destName ) throws SolrServerException {
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
      if ( dest.equals(destName) ) {
        out.add( source );
      }
    }
    return out;
  }


  public static void main( String[] argv ) throws Exception {
    Schema s1 = new SchemaFromRestAdHock( URL1 );
    String report1 = s1.generateReport();
    System.out.println( "====== Report: " + URL1 + " =============");
    System.out.println( report1 );

    Schema s2 = new SchemaFromRestAdHock( URL1 );
    String report2 = s2.generateReport();
    System.out.println( "====== Report: " + URL2 + " =============");
    System.out.println( report2 );

    //    HttpSolrServer s1 = new HttpSolrServer( URL1 );
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
    //      Set<String> tmpDests = getCopyFieldDestinationsForSource( s1, source );
    //      System.out.println( "\tFrom: '"+ source + "' To " + tmpDests );
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
    //      Set<String> tmpSrcs = getCopyFieldSourcesForDestination( s1, dest );
    //      System.out.println( "\tDest: '"+ dest + "' From " + tmpSrcs );
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