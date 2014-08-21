package com.lucidworks.dq.diff;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
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

  static String MODE_REPORT = "full_report";
  static String MODE_A_ONLY = "a_only";
  static String MODE_B_ONLY = "b_only";
  static String MODE_INTERSECT = "intersect";
  static String MODE_UNION = "union";
  static String DEFAULT_MODE = MODE_REPORT;
  static Set<String> VALID_MODES = new LinkedHashSet<String>() {{
    add( MODE_REPORT );
    add( MODE_A_ONLY );
    add( MODE_B_ONLY );
    add( MODE_INTERSECT );
    add( MODE_UNION );
  }};

  public static String getShortDescription() {
    return HELP_WHAT_IS_IT;
  }
  
  public static String NL = System.getProperty("line.separator");

  // command line options
  static Options options;

  static Set<String> readIdsFromFile( File targetFile, CharsetDecoder deccoder ) throws IOException {
    Set<String> ids = new LinkedHashSet<String>();
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(targetFile), deccoder));
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
      // log.info( HELP_WHAT_IS_IT );
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

  public static void main( String[] argv ) throws SolrServerException, IOException {

    options = new Options();
    options.addOption( "u", "url_a", true, "URL for first Solr, Eg http://localhost:8983/solr/collection1, OR set host, port and possibly collection" );
    options.addOption( "h", "host_a", true, "IP address for first Solr, default=localhost" );
    options.addOption( "p", "port_a", true, "Port for first Solr, default=8983" );
    options.addOption( "c", "collection_a", true, "Collection/Core for first Solr, Eg: collection1" );

    options.addOption( "U", "url_b", true, "URL for second Solr, Eg http://localhost:8983/solr/collection2, OR set host, port and possibly collection" );
    options.addOption( "H", "host_b", true, "IP address for second Solr, default=localhost" );
    options.addOption( "P", "port_b", true, "Port for second Solr, default=8983" );
    options.addOption( "C", "collection_b", true, "Collection/Core for second Solr, Eg: collection1" );

    options.addOption( "f", "file_a", true, "Read IDs for A from a text file, one ID per line (skips 0 length lines, not counting newlines)" );
    options.addOption( "F", "file_b", true, "Read IDs for B from a text file, one ID per line (skips 0 length lines, not counting newlines)" );

    options.addOption( "o", "output_file", true, "Output file to create for the full report or ID list (default or \"-\" is stdout / standard out)" );
    options.addOption( "e", "encoding", true, "Character Encoding for reading and writing files (default is UTF-8, which enables cross-platform comparisons)" );
    options.addOption( "l", "loose_encoding", false, "Disable strict character encoding so that problems don't throw Exceptions (NOT recommended)" );

    options.addOption( "m", "mode", true,
        "What to output:"
            + " \"" + MODE_REPORT + "\" means fully formatted report (default)"
            + ", \"" + MODE_A_ONLY + "\" bare list of IDs only in A (one per line)"
            + ", \"" + MODE_B_ONLY + "\" IDs only in B"
            + ", \"" + MODE_INTERSECT + "\" IDs preent in BOTH A AND B"
            + ", \"" + MODE_UNION + "\" IDs in A or B or in both (combines all IDs from both, but each ID will only appear once)"
        );
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
    String fileA = cmd.getOptionValue( "file_a" );
    int optsA = 0;
    optsA += (null!=fullUrlA) ? 1 : 0;
    optsA += (null!=hostA) ? 1 : 0;
    optsA += (null!=fileA) ? 1 : 0;
    if ( optsA < 1 ) {
      helpAndExit( "Must specifify at least url or host or ids file for first Solr instance", 3 );
    }
    if ( optsA > 1 ) {
      helpAndExit( "Can only specifify one of url, host or ids file for first Solr instance", 4 );
    }

    String fullUrlB = cmd.getOptionValue( "url_b" );
    String hostB = cmd.getOptionValue( "host_b" );
    String portB = cmd.getOptionValue( "port_b" );
    String collB = cmd.getOptionValue( "collection_b" );
    String fileB = cmd.getOptionValue( "file_b" );
    int optsB = 0;
    optsB += (null!=fullUrlB) ? 1 : 0;
    optsB += (null!=hostB) ? 1 : 0;
    optsB += (null!=fileB) ? 1 : 0;
    if ( optsB < 1 ) {
      helpAndExit( "Must specifify at least url or host or ids file for second Solr instance", 3 );
    }
    if ( optsB > 1 ) {
      helpAndExit( "Can only specifify one of url, host or ids file for second Solr instance", 4 );
    }

    // VALID_MODES
    String mode = cmd.getOptionValue( "mode" );
    if ( null!=mode ) {
      mode = mode.toLowerCase().trim();
      if ( ! VALID_MODES.contains(mode) ) {
        helpAndExit( "Invalid mode, must be one of: " + VALID_MODES, 5 );
      }
    }
    boolean isNormalReport = (null==mode) || mode.equals( MODE_REPORT );

    // File IO
    String outputFile = cmd.getOptionValue( "output_file" );
    String encodingStr = cmd.getOptionValue( "encoding" );
    if ( null==encodingStr || encodingStr.trim().length()<1 ) {
      encodingStr = "UTF-8";
    }
    boolean strictEncoding = true;
    if(cmd.hasOption("loose_encoding")) {
      strictEncoding = false;
    }

    // Setup IO encoding
    Charset charset = Charset.forName( encodingStr );
    // Input uses Decoder
    CharsetDecoder decoder = charset.newDecoder();
    // Output uses Encoder
    CharsetEncoder encoder = charset.newEncoder();
    if ( strictEncoding ) {
      decoder.onMalformedInput( CodingErrorAction.REPORT );
      encoder.onMalformedInput( CodingErrorAction.REPORT );
    }

    PrintWriter out = null;
    if( null!=outputFile && ! outputFile.equals("-") ) {
      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), encoder), true);
    } else {
      out = new PrintWriter(new OutputStreamWriter(System.out, encoder), true);
    }

    // Init
    // HttpSolrServer solrA = new HttpSolrServer( URL1 );
    HttpSolrServer solrA = null;
    if ( null==fileA ) {
      if ( null!=fullUrlA ) {
        solrA = SolrUtils.getServer( fullUrlA );
      }
      else {
        // Utils handle null values
        solrA = SolrUtils.getServer( hostA, portA, collA );    
      }
      if(isNormalReport) out.println( "First Solr / Solr A = " + solrA.getBaseURL() );
    }
    else {
      if(isNormalReport) out.println( "First Solr / Solr A read from file = " + fileA );
    }

    // HttpSolrServer solrB = new HttpSolrServer( URL2 );
    HttpSolrServer solrB = null;
    if ( null==fileB ) {
      if ( null!=fullUrlB ) {
        solrB = SolrUtils.getServer( fullUrlB );
      }
      else {
        // Utils handle null values
        solrB = SolrUtils.getServer( hostB, portB, collB );    
      }
      if(isNormalReport) out.println( "Second Solr / Solr B = " + solrB.getBaseURL() );
    }
    else {
      if(isNormalReport) out.println( "Second Solr / Solr B read from file = " + fileB );
    }

    Set<String> idsA = (null!=solrA) ? SolrUtils.getAllIds( solrA ) : readIdsFromFile( new File(fileA), decoder );
    Set<String> idsB = (null!=solrB) ? SolrUtils.getAllIds( solrB ) : readIdsFromFile( new File(fileB), decoder );

    if ( isNormalReport ) {
      // Use non-destructive here since we use the lists more than once
      Set<String> aOnly = SetUtils.inAOnly_nonDestructive(idsA, idsB);
      Set<String> bOnly = SetUtils.inBOnly_nonDestructive(idsA, idsB);
      out.println( "A-only: " + aOnly );
      out.println( "B-only: " + bOnly );
    }
    else {
      Set<String> ids = null;
      if ( mode.equals(MODE_A_ONLY) ) {
        // destructive OK here since we're just doing 1 calculation
        ids = SetUtils.inAOnly_destructive( idsA, idsB );
      }
      else if ( mode.equals(MODE_B_ONLY) ) {
        ids = SetUtils.inBOnly_destructive( idsA, idsB );
      }
      else if ( mode.equals(MODE_INTERSECT) ) {
        ids = SetUtils.intersection_destructive( idsA, idsB );  
      }
      else if ( mode.equals(MODE_UNION) ) {
        ids = SetUtils.union_destructive( idsA, idsB );  
      }
      else {
        // This should never happen.
        // If it ever does, maybe somebody added to VALID_MODES but didn't add a case here
        throw new IllegalStateException( "Unknown mode \"" + mode + "\", check VALID_MODES" );
      }

      // Print the results
      for ( String id : ids ) {
        out.println( id );
      }
    }
    out.close();
  }

}
