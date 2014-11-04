package com.lucidworks.dq.zk_experiment;

import java.io.IOException;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

/*
 * Demonstrate a ZooKeeper aware "Smart Client" that can automatically handle server issues
 */
public class ZkSmartClient {
  static final String COLLECTION = "collection1";
  static final String ID_FIELD = "id";

  // BEFORE: Would normally use this w/ HttpSolrServer
  static final String SOLR_URL = "http://localhost:8983/solr/" + COLLECTION;

  // AFTER: Instead we use this w/ CloudSolrServer
  // These are ZooKeeper instances, could map to 1 or 100 Solr servers
  static final String ZK_ENSEMBLE = "localhost:2181,localhost:2182,localhost:2183";
 
  static SolrServer openServer() {

    // BEFORE: Normally we'd use this:
    // HttpSolrServer extends SolrServer
    // HttpSolrServer server = new HttpSolrServer( serverUrl );
    
    // AFTER: Instead we use this:
    // CloudSolrServer extends SolrServer
    CloudSolrServer server = new CloudSolrServer( ZK_ENSEMBLE );
    // .setDefaultColl not defined for base SolrServer type
    server.setDefaultCollection( COLLECTION );

    return server;
  }

  static void addDoc( SolrServer server, int id ) throws SolrServerException, IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField( ID_FIELD, "" + id );
    doc.addField( "name", "Test Doc " + id );
    server.add(doc);
    // Normally wouldn't do this, but OK for small test
    server.commit();  
    System.out.println( "Added doc " + id );
  }
  
  static void testSearch( SolrServer server ) throws SolrServerException {
    SolrQuery query = new SolrQuery( "*:*" );
    query.addField( ID_FIELD );
    QueryResponse res = server.query( query );
    System.out.println( "Sample doc IDs:" );
    // gets max of 10 docs by default
    for ( SolrDocument doc : res.getResults() ) {
      String id = (String) doc.get( ID_FIELD );
      System.out.println( id );
    }
  }

  // Similar to using REST call:
  // http://localhost:8983/solr/admin/collections?action=CREATE&name=MyColl&numShards=1&maxShardsPerNode=1&replicationFactor=1&collection.configName=MyConfig

  static void createCollection( String zooKeeperConnectString, String newCollectionName ) {
    CloudSolrServer server = new CloudSolrServer( zooKeeperConnectString );
    // server.setDefaultCollection( newCollectionName );
    server.connect();
    try {
      // CoreAdminRequest.Create req = new CoreAdminRequest.Create();
      // It's Collections, not Cores!  Cores are old-school
      CollectionAdminRequest.Create req = new CollectionAdminRequest.Create();

      // req.setCoreName( newCollectionName );
      req.setCollectionName( newCollectionName );

      // Don't use this stuff for Collections, only for Cores
      //req.setInstanceDir( "/home/solr_data/solr/" );
      //req.setDataDir( newCollectionName );
      //req.setIsTransient( true );

      //req.setNumShards( 2 );
      req.setNumShards( 1 );

      req.setConfigName( "MyConf" );
      //req.setConfigSet( "MyConf" );

      req.process( server );
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }

  }

  public static void main(String[] args) throws SolrServerException, IOException {
//    SolrServer server = openServer();
//    addDoc( server, 4 );
//    testSearch( server );

    createCollection( ZK_ENSEMBLE, "MyColl4" );
  
  }

}
