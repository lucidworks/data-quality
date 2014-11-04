package com.lucidworks.dq.logs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

public class LogFileBase implements LogFile {
  
  // TODO: could leave this NULL until they've called .process() ?
  Collection<LogEntry> entries = new ArrayList<>();
  File sourceFile;

  // Public "factory" methods
  public static LogFile logFileFromDiskFile( File inFile ) throws IOException {
    return new LogFileBase( inFile );
  }
  public static LogFile logFileFromDiskFile( String fileName ) throws IOException {
    return new LogFileBase( new File(fileName) );
  }

  LogFileBase( File sourceFile ) {
    this.sourceFile = sourceFile;
  }

  // Break out processing logic out from constructor
  // in case we want to defer it
  @Override
  public void read() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "UTF-8"));
    while( true ) {
        String line = in.readLine();
        if ( null==line ) {
            break;
        }
        LogEntry entry = LogEntryBase.logEntryFromString( line );
        entries.add( entry );
    }
    in.close();
  }
  
  @Override
  public Collection<LogEntry> getEntries() {
    return entries;
  }


  public static void main(String[] args) throws IOException {
    for ( int i=0; i<args.length; i++ ) {
      LogFile entry = logFileFromDiskFile( args[i] );
      System.out.println( entry );
    }
  }


}
