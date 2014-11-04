package com.lucidworks.dq.logs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.lucidworks.dq.util.SetUtils;

public class LogFileRepoBase implements LogFileRepo {

  Collection<File> myQueue = new ConcurrentLinkedQueue<>();
 
  File startingDirOrFile;

  // Regex, Optional
  String includePattern;
  
  boolean shouldIncludeCompressedFiles;

  public LogFileRepoBase( String startingDirOrFile ) {
    this( new File(startingDirOrFile) );
  }
  public LogFileRepoBase( File startingDirOrFile ) {
    this.startingDirOrFile = startingDirOrFile;    
  }

  @Override
  public Collection<LogFile> findLogFiles() {
    traverse( myQueue, startingDirOrFile );
    Collection<LogFile> outList = new ArrayList<>();
    for ( File f : myQueue ) {
      LogFile lf = new LogFileBase( f );
      outList.add( lf );
    }
    return outList;
  }

  @Override
  public void setIncludePattern(String pattern) {
    this.includePattern = pattern;
  }
  @Override
  public String getIncludePattern() {
    return includePattern;
  }

  @Override
  public void setIncludeCompressedFiles(boolean flag) {
    this.shouldIncludeCompressedFiles = flag;
  }
  @Override
  public boolean getIncludeCompressedFiles() {
    return shouldIncludeCompressedFiles;
  }
  
  //Lookup all the files
  //traverse( myQueue, "someDirName", null );
  //Or simpler
  //Collection files = LinkedHashSet<File>();
  //traverse( files, "someDirName", null );

  //TODO: would be better to pass in method to call
  void traverse( Collection<File>queue, String startDir ) {
    traverse( queue, new File(startDir) );
  }
  void traverse( Collection<File>queue, File candidate ) {
    if( candidate.isFile() ) {
      if ( null==getIncludePattern() || candidate.toString().matches(getIncludePattern()) ) {
        queue.add( candidate );
      }
    }
    // Else probably a directory
    else if ( candidate.isDirectory() ) {
      File [] entries = candidate.listFiles();
      for ( File f : entries ) {
        traverse( queue, f );
      }
    }
    else {
      System.out.println( "ERROR: Neither file nor directory: " + candidate );
    }
  }

  public static void main(String[] args) throws IOException {
    // Moved to LogEntryFromSolr main

//    for ( int i=0; i<args.length; i++ ) {
//      LogFileRepo repo = new LogFileRepoBase( args[i] );
//      Collection<LogFile> logs = repo.findLogFiles();
//      for ( LogFile lf : logs ) {
//        lf.read();
//        Collection<LogEntry> rawEntries = lf.getEntries();
//        Collection<LogEntryFromSolr> solrEntries = new ArrayList<>();
//        for ( LogEntry rawEntry : rawEntries ) {
//          // LogEntryFromSolr solrEntry = new LogEntryFromSolr( rawEntry );
//          LogEntry solrEntry = LogEntryFromSolr.solrLogEntryFromBaseEntryOrNull( rawEntry );
//          // if ( solrEntry.isSolrPattern() )
//          if ( null != solrEntry )
//          {
//            solrEntries.add( (LogEntryFromSolr) solrEntry );
//          }
//        }
//        Map<String,Long> queryTypeCounts = LogEntryFromSolr.tabulateQueryArgCombos( solrEntries );
//        // composite-parameter-key -> each-parameter-name-> unique-value -> count
//        Map<String,Map<String,Map<String,Long>>> detailedStats = LogEntryFromSolr.tabulateQueryArgCombosAndValues( solrEntries );
//        queryTypeCounts = SetUtils.sortMapByValues( queryTypeCounts );
//        queryTypeCounts = SetUtils.reverseMapEntryKeyOrder( queryTypeCounts );
//        for ( Entry<String, Long> e1 : queryTypeCounts.entrySet() ) {
//          String queryType = e1.getKey();
//          Long queryTypeCount = e1.getValue();
//          System.out.println( "" + queryTypeCount + " " + queryType );
//          Map<String,Map<String,Long>> statsForQueryType = detailedStats.get( queryType );
//          for ( Entry<String, Map<String, Long>> e2 : statsForQueryType.entrySet() ) {
//            String paramName = e2.getKey();
//            System.out.println( "\t" + paramName + ":" );
//            Map<String, Long> paramValues = e2.getValue();
//            paramValues = SetUtils.sortMapByValues( paramValues );
//            paramValues = SetUtils.reverseMapEntryKeyOrder( paramValues );
//            for ( Entry<String, Long> e3 : paramValues.entrySet() ) {
//              String value = e3.getKey();
//              Long valueCount = e3.getValue();
//              System.out.println( "\t\t" + valueCount + " " + value );
//            }
//          }
//        }
//      }
//      // System.out.println( repo );
//    }

  }
  
  
}
