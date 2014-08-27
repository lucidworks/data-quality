package com.lucidworks.dq.schema;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class SchemaFromRest extends SchemaBase implements Schema {

  // Other constant fields at bottom after main()

  static String ID_FIELD = "id";
  // ^--ID also assumed to be a string
  static int ROWS = Integer.MAX_VALUE;

  static String NOT_AVAILABLE = "(not-available)";
  // Some data not available, Eg:
  // getSimilarityModelClassName() - works in AdHock
  // getDefaultOperator()          - works in AdHock
  // getDefaultSearchField()       - not sure you can get this via REST
  
  static String DEFAULT_HOST = "localhost";
  static String DEFAULT_PORT = "8983";
  static String DEFAULT_COLL = "collection1"; // "demo_shard1_replica1";
  static String DEFAULT_URL = "http://" + DEFAULT_HOST + ":" + DEFAULT_PORT + "/solr/" + DEFAULT_COLL;
  // + "/select?q=*:*&rows=" + ROWS + "&fl=id&wt=json&indent=on"

  HttpSolrServer server;
  // NamedList<Object> schema;
  // Collection<SimpleOrderedMap> schema;
  // ^-- java.lang.ClassCastException: org.apache.solr.common.util.SimpleOrderedMap cannot be cast to java.util.Collection
  SimpleOrderedMap schema;

  public SchemaFromRest() throws MalformedURLException, SolrServerException {
    this( DEFAULT_URL );
  }

  // Can't do:
  // public SchemaFromRest( String host )
  // because it conflicts with  SchemaFromRest( String serverUrl )

  public SchemaFromRest( String host, String port ) throws SolrServerException {
    this( host, port, DEFAULT_COLL );
  }
  public SchemaFromRest( String host, int port ) throws SolrServerException {
    this( host, port, DEFAULT_COLL );
  }
  public SchemaFromRest( String host, int port, String collection ) throws SolrServerException {
    this( host, ""+port, collection );
  }
  public SchemaFromRest( String host, String port, String collection ) throws SolrServerException {
    String url = "http://" + host + ":" + port + "/solr/" + collection;
    init( url );
  }

  public SchemaFromRest( URL serverUrl ) throws SolrServerException {
    init( serverUrl.toExternalForm() );
  }
  public SchemaFromRest( String serverUrl ) throws MalformedURLException, SolrServerException {
    init( serverUrl );
  }
  public SchemaFromRest( HttpSolrServer solrServer ) throws MalformedURLException, SolrServerException {
    init( solrServer );
  }

  void init( String serverUrl ) throws SolrServerException {
    HttpSolrServer solrServer = new HttpSolrServer( serverUrl );
    init( solrServer );
  }
  void init( HttpSolrServer server ) throws SolrServerException {
    this.server = server;
    SolrQuery q = new SolrQuery();
    // https://cwiki.apache.org/confluence/display/solr/Schema+API
    q.setRequestHandler("/schema"); 
    QueryResponse res = server.query( q );
    NamedList<Object> res2 = res.getResponse();
    schema = (SimpleOrderedMap)res2.get("schema");
  }

  public Set<String> getIds() throws SolrServerException {
    Set<String> out = new LinkedHashSet<>();
    SolrQuery q = new SolrQuery( "*:*" );
    q.addField( ID_FIELD );
    q.setRows( ROWS );
    QueryResponse res = server.query( q );
    for ( SolrDocument doc : res.getResults() ) {
      String id = (String) doc.get( ID_FIELD );
      out.add( id );
    }
    return out;
  }

  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getSchemaVersion()
   */
  @Override
  public float getSchemaVersion() {
    float version = (float) schema.get("version");
    return version;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getSchemaName()
   */
  @Override
  public String getSchemaName() {
    String name = (String) schema.get("name");
    return name;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getUniqueKeyFieldName()
   */
  @Override
  public String getUniqueKeyFieldName() {
    String key = (String) schema.get("uniqueKey");
    return key;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getSimilarityModelClassName()
   */
  @Override
  public String getSimilarityModelClassName() {
    //      NamedList<Object> sim = (NamedList<Object>) schema.get("similarity");
    //      String className = (String) sim.get("class");
    //      return className;
    return NOT_AVAILABLE;
    //    // TODO: Alt call
    //    SolrQuery q = new SolrQuery();
    //    q.setRequestHandler("/schema/similarity");
    //    QueryResponse res = server.query( q );
    //    NamedList<Object> res2 = res.getResponse();
    //    NamedList<Object> sim = (NamedList<Object>) res2.get("similarity");
    //    String className = (String) sim.get("class");
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getDefaultOperator()
   */
  @Override
  public String getDefaultOperator() {
    // q.setRequestHandler("/schema/solrqueryparser/defaultoperator");
    //      Object obj1 = schema.get("defaultOperator");
    //      Object obj2 = schema.get("solrqueryparser");
    //      String op = (String) schema.get("defaultOperator");
    //      return op;
    return NOT_AVAILABLE;
    //    // TODO: Alt REST call
    //    SolrQuery q = new SolrQuery();
    //    q.setRequestHandler("/schema/solrqueryparser/defaultoperator");
    //    QueryResponse res = server.query( q );
    //    NamedList<Object> res2 = res.getResponse();
    //    String op = (String) res2.get("defaultOperator");
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getDefaultSearchField()
   */
  @Override
  public String getDefaultSearchField() {
    return NOT_AVAILABLE;
    // TODO: REST Call for this?
  }

  public Map<String, Set<String>> getAllDeclaredAndDynamicFieldsByType() {
    Map<String, Set<String>> out = new LinkedHashMap<>();

    // Declared Field Names
    // q.setRequestHandler("/schema/fields");
    Collection<SimpleOrderedMap> fields1 = (Collection<SimpleOrderedMap>) schema.get("fields");
    for ( SimpleOrderedMap f : fields1 ) {
      //System.out.println( "f=" + f );
      String name = (String)f.get( "name" );
      String type = (String)f.get( "type" );
      utilTabulateFieldTypeAndName( out, type, name );
    }

    // Dynamic Fields
    // q.setRequestHandler("/schema/dynamicfields");
    Collection<SimpleOrderedMap> fields2 = (Collection<SimpleOrderedMap>) schema.get("dynamicFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields2 ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "name" );
      String type = (String)f.get( "type" );
      utilTabulateFieldTypeAndName( out, type, name );
    }
    return out;
  }

  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getAllSchemaFieldNames()
   */
  @Override
  public Set<String> getAllSchemaFieldNames() {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/fields");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("fields");
    //System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      //System.out.println( "f=" + f );
      String name = (String)f.get( "name" );
      out.add( name );
    }
    return out;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getAllDynamicFieldPatterns()
   */
  @Override
  public Set<String> getAllDynamicFieldPatterns() {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/dynamicfields");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("dynamicFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "name" );
      out.add( name );
    }
    return out;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getAllFieldTypeNames()
   */
  @Override
  public Set<String> getAllFieldTypeNames() {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/fieldtypes");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("fieldTypes");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "name" );
      out.add( name );
    }
    return out;
  }

  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getAllCopyFieldSourceNames()
   */
  @Override
  public Set<String> getAllCopyFieldSourceNames() {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/copyfields");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "source" );
      out.add( name );
    }
    return out;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getAllCopyFieldDestinationNames()
   */
  @Override
  public Set<String> getAllCopyFieldDestinationNames() {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/copyfields");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("copyFields");
    // System.out.println( "fields=" + fields );
    for ( SimpleOrderedMap f : fields ) {
      // System.out.println( "f=" + f );
      String name = (String)f.get( "dest" );
      out.add( name );
    }
    return out;
  }
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getCopyFieldDestinationsForSource(java.lang.String)
   */
  @Override
  public Set<String> getCopyFieldDestinationsForSource( String sourceName ) {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/copyfields");
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("copyFields");
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
  /* (non-Javadoc)
   * @see com.lucidworks.dq.diff.Schema#getCopyFieldSourcesForDestination(java.lang.String)
   */
  @Override
  public Set<String> getCopyFieldSourcesForDestination( String destName ) {
    Set<String> out = new LinkedHashSet<>();
    // q.setRequestHandler("/schema/copyfields"); 
    Collection<SimpleOrderedMap> fields = (Collection<SimpleOrderedMap>) schema.get("copyFields");
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
    Schema s = new SchemaFromRest( HOST0, PORT0, COLL0 );
    String report = s.generateReport();
    System.out.println( report );
  }

  public static void _main_v1( String[] argv ) throws Exception {
    Schema s = new SchemaFromRest( HOST0, PORT0, COLL0 );

    // Singular Values
    float vers = s.getSchemaVersion();
    System.out.println( "Schema Version: " + vers );
    String name = s.getSchemaName();
    System.out.println( "Schema Name: " + name );
    String key = s.getUniqueKeyFieldName();
    System.out.println( "Key Field: " + key );
    String defOp = s.getDefaultOperator();
    System.out.println( "Default Operator: " + defOp );
    String sim = s.getSimilarityModelClassName();
    System.out.println( "Similarity Class Name: " + sim );
    String defField = s.getDefaultSearchField();
    System.out.println( "Default Search Field: " + defField );

    // Complex Values
    Set<String> fields = s.getAllSchemaFieldNames();
    System.out.println( "Fields: " + fields );
    Set<String> dynFields = s.getAllDynamicFieldPatterns();
    System.out.println( "Dynamic field Patterns: " + dynFields );

    Set<String> typeNames = s.getAllFieldTypeNames();
    System.out.println( "Types: " + typeNames );

    Set<String> sourceNames = s.getAllCopyFieldSourceNames();
    System.out.println( "Copy Sources: " + sourceNames );
    for ( String source : sourceNames ) {
      Set<String> tmpDests = s.getCopyFieldDestinationsForSource(source);
      System.out.println( "\tFrom: '"+ source + "' To " + tmpDests );
    }

    Set<String> destNames = s.getAllCopyFieldDestinationNames();
    System.out.println( "Copy Destinations: " + destNames );
    for ( String dest : destNames ) {
      Set<String> tmpSrcs = s.getCopyFieldSourcesForDestination( dest );
      System.out.println( "\tDest: '"+ dest + "' From " + tmpSrcs );
    }
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