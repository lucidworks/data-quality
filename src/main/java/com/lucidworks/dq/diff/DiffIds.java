package com.lucidworks.dq.diff;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DiffIds /*implements HasDescription*/ {
  static String HELP_WHAT_IS_IT = "Compare IDs between two cores/collections.";
  static String HELP_USAGE = "DiffIds";
  // final static Logger log = LoggerFactory.getLogger( TermStats.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }
  
  static Options options;

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

    options = new Options();
	options.addOption( "u", "url_a", true, "URL for first Solr, OR set host, port and possibly collection" );
	options.addOption( "h", "host_a", true, "IP address for first Solr, default=localhost" );
	options.addOption( "p", "port_a", true, "Port for first Solr, default=8983" );
	options.addOption( "c", "collection_a", true, "Collection/Core for first Solr, Eg: collection1" );
	options.addOption( "U", "url_b", true, "URL for second Solr, OR set host, port and possibly collection" );
	options.addOption( "H", "host_b", true, "IP address for second Solr, default=localhost" );
	options.addOption( "P", "port_b", true, "Port for second Solr, default=8983" );
	options.addOption( "C", "collection_b", true, "Collection/Core for second Solr, Eg: collection1" );

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

    String fullUrlA = cmd.getOptionValue( "url_a" );
    String hostA = cmd.getOptionValue( "host_a" );
    String portA = cmd.getOptionValue( "port_a" );
    String collA = cmd.getOptionValue( "collection_a" );
    if ( null==fullUrlA && null==hostA ) {
      helpAndExit( "Must specifify at least url or host for first Solr", 3 );
    }
    if ( null!=fullUrlA && null!=hostA ) {
      helpAndExit( "Must not specifify both url and host for first Solr", 4 );
    }

    String fullUrlB = cmd.getOptionValue( "url_b" );
    String hostB = cmd.getOptionValue( "host_b" );
    String portB = cmd.getOptionValue( "port_b" );
    String collB = cmd.getOptionValue( "collection_b" );
    if ( null==fullUrlB && null==hostB ) {
      helpAndExit( "Must specifify at least url or host for second Solr", 3 );
    }
    if ( null!=fullUrlB && null!=hostB ) {
      helpAndExit( "Must not specifify both url and host for second Solr", 4 );
    }


    // Init
	// HttpSolrServer solrA = new HttpSolrServer( URL1 );
    HttpSolrServer solrA;
    if ( null!=fullUrlA ) {
      solrA = SolrUtils.getServer( fullUrlA );
    }
    else {
      // Utils handle null values
      solrA = SolrUtils.getServer( hostA, portA, collA );    
    }
    System.out.println( "First Solr / Solr A = " + solrA.getBaseURL() );

    // HttpSolrServer solrB = new HttpSolrServer( URL2 );
    HttpSolrServer solrB;
    if ( null!=fullUrlB ) {
      solrB = SolrUtils.getServer( fullUrlB );
    }
    else {
      // Utils handle null values
      solrB = SolrUtils.getServer( hostB, portB, collB );    
    }
    System.out.println( "Second Solr / Solr B = " + solrB.getBaseURL() );

    Set<String> aOnly = idsAOnly_fromServers( solrA, solrB );
    Set<String> bOnly = idsBOnly_fromServers( solrA, solrB );
    
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