package com.lucidworks.dq.diff;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DiffIds {

  // Constant fields at the bottom, after main
  
  // Server Based
  // Since we re-query, we allow it to be destructive
  public static Set<String> idsAOnly_fromServers( HttpSolrServer serverA, HttpSolrServer serverB ) throws SolrServerException {
	Set<String> idsA = SolrUtils.getAllIds( serverA );
	Set<String> idsB = SolrUtils.getAllIds( serverB );
	return SetUtils.inAOnly_destructive( idsA, idsB );
  }
  public static Set<String> idsBOnly_fromServers( HttpSolrServer serverA, HttpSolrServer serverB ) throws SolrServerException {
	return idsAOnly_fromServers( serverB, serverA );
  }
  public static Set<String> idsIntersection_fromServers( HttpSolrServer serverA, HttpSolrServer serverB ) throws SolrServerException {
    Set<String> idsA = SolrUtils.getAllIds( serverA );
	Set<String> idsB = SolrUtils.getAllIds( serverB );
	return SetUtils.intersection_destructive( idsA, idsB );
  }
  public static Set<String> idsUnion_fromServers( HttpSolrServer serverA, HttpSolrServer serverB ) throws SolrServerException {
	Set<String> idsA = SolrUtils.getAllIds( serverA );
	Set<String> idsB = SolrUtils.getAllIds( serverB );
	return SetUtils.union_destructive( idsA, idsB );
  }
  
  
  // Higher Level

  public static void diffIds( HttpSolrServer serverA, HttpSolrServer serverB,
		  Set<String> optAOnly, Set<String> optBOnly,
		  Set<String> optInBoth, Set<String> optUnion
  ) throws SolrServerException {
	  // TODO: could optimize if only looking for 1 of them
	  Set<String> idsA = SolrUtils.getAllIds( serverA );
	  Set<String> idsB = SolrUtils.getAllIds( serverB );
	  if ( null!=optAOnly ) {
		  optAOnly.addAll( idsA );
		  optAOnly.removeAll( idsB );
	  }
	  if ( null!=optBOnly ) {
		  optBOnly.addAll( idsB );
		  optBOnly.removeAll( idsA );
	  }
	  if ( null!=optInBoth ) {
		  optInBoth.addAll( idsA );
		  optInBoth.retainAll( idsB );
	  }
	  if ( null!=optUnion ) {
		  optUnion.addAll( idsA );
		  optUnion.addAll( idsB );
	  }
  }
  
  public static void main( String[] argv ) throws SolrServerException {
    /**
	System.out.println( "Getting IDs from "+URL0 + " ..." );
    HttpSolrServer s0 = new HttpSolrServer( URL0 );
    long start = System.currentTimeMillis();
    Set<String> ids = getIds( s0 );
    long end = System.currentTimeMillis();
    long duration = end - start;
    System.out.println( "Got " + ids.size() + " docs in " + duration + " ms" );
    **/
    HttpSolrServer s1 = new HttpSolrServer( URL1 );
    HttpSolrServer s2 = new HttpSolrServer( URL2 );
    Set<String> aOnly = idsAOnly_fromServers( s1, s2 );
    Set<String> bOnly = idsBOnly_fromServers( s1, s2 );
    
    System.out.println( "A-only: " + aOnly );
    System.out.println( "B-only: " + bOnly );
    
    
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