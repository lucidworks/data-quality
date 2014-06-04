package com.lucidworks.dq.data;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DumpIds /*implements HasDescription*/ {

  static String HELP_WHAT_IS_IT = "Dump all the IDs from a collection to standard out / stdout.";
  static String HELP_USAGE = "DumpIds -u http://localhost:8983 (output sent to stdout)";
  // final static Logger log = LoggerFactory.getLogger( FieldStats.class );

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  static Options options;
  
  HttpSolrServer solrServer;

  // TODO: refactor to allow options to be settable after constructor is run
  public DumpIds( HttpSolrServer server ) throws SolrServerException {
    this.solrServer = server;
  }
  public HttpSolrServer getSolrServer() {
	return this.solrServer;
  }

  void dumpIds() throws SolrServerException {
	Set<String> ids = SolrUtils.getAllIds( getSolrServer() );
	for ( String id : ids ) {
		System.out.println( id );
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

  public static void main( String [] argv ) throws Exception {

	options = new Options();
	options.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
	options.addOption( "h", "host", true, "IP address for Solr, default=localhost but still required of no other args passed" );
	options.addOption( "p", "port", true, "Port for Solr, default=8983" );
	options.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
    if ( argv.length < 1 ) {
      helpAndExit( "Must specifify at least url or host", 1 );
    }
    CommandLine cmd = null;
    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse( options, argv );
    }
    catch( ParseException exp ) {
      helpAndExit( "Parsing command line failed. Reason: " + exp.getMessage(), 2 );
    }
    String fullUrl = cmd.getOptionValue( "url" );
    String host = cmd.getOptionValue( "host" );
    String port = cmd.getOptionValue( "port" );
    String coll = cmd.getOptionValue( "collection" );
    if ( null==fullUrl && null==host ) {
      helpAndExit( "Must specifify at least url or host (b)", 3 );
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
    
    // System.out.println( "Solr = " + solr.getBaseURL() );
	// EmptyFieldStats fs = new EmptyFieldStats( solr );
	DumpIds di = new DumpIds( solr );
	di.dumpIds();
  
  }
}