package com.lucidworks.dq.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lucidworks.dq.util.HasDescription;
import com.lucidworks.dq.util.SetUtils;
import com.lucidworks.dq.util.SolrUtils;

public class DeleteByIds /*implements HasDescription*/ {

  static String HELP_WHAT_IS_IT = "Delete documents by their ID, either passed on the command line, or from a file, or from standard in / stdin.";
  static String HELP_USAGE = "DeleteByIds -u http://localhost:8983/collection1 --ids 1234 5678 ... or --input_file ids_to_delete.txt";

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }

  static int DEFAULT_BATCH_SIZE = 1000;

  static Options options;

  // We use List<String> instead of Set<String> because that's what SolrJ expects in deleteById
  static List<String> readIdsFromFile( String targetFile, CharsetDecoder deccoder ) throws IOException {
    List<String> ids = new LinkedList<String>();
    BufferedReader in = null;
    if( null!=targetFile && ! targetFile.equals("-") ) {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(targetFile), deccoder));
    } else {
      in = new BufferedReader(new InputStreamReader(System.in, deccoder));
    }
    String line;
    while ((line = in.readLine()) != null) {
      // skip completely blank lines, but doesn't do any trimming
      if ( line.length()<1 ) {
          continue;
	  }
	  ids.add( line );
	}
	in.close();
	return ids;
  }

  static void helpAndExit() {
	helpAndExit( null, 1 );
  }
  static void helpAndExit( String optionalError, int errorCode ) {
    HelpFormatter formatter = new HelpFormatter();
    if ( null==optionalError ) {
      System.err.println( HELP_WHAT_IS_IT );
	}
	else {
	  // log.error( optionalError );
	  System.err.println( optionalError );
	}
    // stdout
	//formatter.printHelp( HELP_USAGE, options, true );
    // stderr
    PrintWriter pw = new PrintWriter(System.err);
	formatter.printHelp( pw, 78, HELP_USAGE, null, options, 1, 1, null, true );
	pw.flush();
	System.exit( errorCode );
  }

  public static void main( String [] argv ) throws Exception {

	options = new Options();
	options.addOption( "u", "url", true, "URL for Solr, OR set host, port and possibly collection" );
	options.addOption( "h", "host", true, "IP address for Solr, default=localhost but still required of no other args passed" );
	options.addOption( "p", "port", true, "Port for Solr, default=8983" );
	options.addOption( "c", "collection", true, "Collection/Core for Solr, Eg: collection1" );
	options.addOption( "f", "input_file", true, "File to read IDs from, one ID per line (skips 0 length lines, not counting newlines) (Use \"-\" for stdout / standard out)" );
	options.addOption( "e", "encoding", true, "Character Encoding for reading and writing files (default is UTF-8, which enables cross-platform comparisons)" );
	options.addOption( "l", "loose_encoding", false, "Disable strict character encoding so that problems don't throw Exceptions (NOT recommended)" );

	options.addOption( OptionBuilder.withLongOpt( "batch_size" )
            .withDescription( "Batch size, 1=doc-by-doc, 0=all-at-once (be careful memory-wise), default="+DEFAULT_BATCH_SIZE )
            .hasArg()
            .withType( Number.class ) // NOT Long.class
            .create( "b" )
            );

	options.addOption( OptionBuilder.withLongOpt( "ids" )
            .withDescription( "Pass one or more IDs on the command line" )
            .hasArgs()   // PLURAL!
            .create( "i" )
            );

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

    int batchSize = DEFAULT_BATCH_SIZE;
    Integer batchObj = (Integer) cmd.getParsedOptionValue( "batch_size" );
    if ( null!=batchObj ) {
      if ( batchObj.longValue() < 0L ) {
    	helpAndExit( "batch_size must be >= 0", 5 );	
      }
      batchSize = batchObj.intValue();
    }

    String encodingStr = cmd.getOptionValue( "encoding" );
    // Didn't set encoding
    if ( null==encodingStr || encodingStr.trim().length()<1 ) {
      encodingStr = "UTF-8";
    }
    // Did set encoding
    else {
      // But didn't set input file
      if ( null == cmd.getOptionValue( "input_file" ) ) {
      	helpAndExit( "Encoding only applicable when reading from input file or standard in / stdiin; operating system handles command line argument encoding", 6 );	    	  
      }
    }
    boolean strictEncoding = true;
    if(cmd.hasOption("loose_encoding")) {
      strictEncoding = false;
      if ( null == cmd.getOptionValue( "input_file" ) ) {
      	helpAndExit( "loose_encoding only applicable when reading from input file or standard in / stdiin; operating system handles command line argument encoding", 7 );	    	  
      }
    }
    // Setup IO encoding
    Charset charset = Charset.forName( encodingStr );
    // Input uses Decoder
    CharsetDecoder decoder = charset.newDecoder();
    if ( strictEncoding ) {
      decoder.onMalformedInput( CodingErrorAction.REPORT );
    }
    
    String inputFile = cmd.getOptionValue( "input_file" );

    String [] cmdLineIds = cmd.getOptionValues( "ids" );

    if ( null==inputFile && null==cmdLineIds ) {
      helpAndExit( "Must use at least one of --input_file or --ids ..., OK to use both. For standard in / stdin use --input_file -", 8 );	    	      	
    }

    // We use List<String> instead of Set<String> because that's what SolrJ expects in deleteById
    List<String> ids = new LinkedList<String>();
    if ( null!=inputFile ) {
      ids = readIdsFromFile( inputFile, decoder );
    }
    if ( null!=cmdLineIds ) {
      ids.addAll( Arrays.asList( cmdLineIds ) );
    }
    
    if ( batchSize < 1 ) {
      solr.deleteById(ids);
    }
    else if ( batchSize == 1 ) {
      for ( String id : ids ) {
    	solr.deleteById( id );
      }
    }
    else {
      for ( int start = 0; start < ids.size(); start += batchSize ) {
    	int end = start + batchSize;
    	if ( end > ids.size() ) {
    	  end = ids.size();
    	}
    	List<String> sublist = ids.subList( start, end );
        solr.deleteById( sublist );
      }
    }
    // Wait for disk commit and new searcher to fire up
    // TODO: maybe have other commit options, although this is probably the safest
    solr.commit( true, true );

  }
}