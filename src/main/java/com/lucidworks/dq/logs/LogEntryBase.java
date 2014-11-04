package com.lucidworks.dq.logs;

import java.util.ArrayList;
import java.util.Collection;

public class LogEntryBase implements LogEntry {

  String rawText;
  Collection<LogEntryReference> references = new ArrayList<>();

  LogEntryBase( String rawText ) {
    this.rawText = rawText;
  }

  @Override
  public String getRawText() {
    return rawText;
  }
  public void setRawText( String rawText ) {
    this.rawText = rawText;
  }

  public static LogEntry logEntryFromString( String rawText ) {
    return new LogEntryBase( rawText );
  }


  @Override
  public Collection<LogEntryReference> getReferences() {
    return references;
  }

  // @Override
  public void addReference(LogEntryReference ref) {
    references.add( ref );
  }

  /*
   * Throw exception so that derived classes are allowed to do so
   */
  public static void main(String[] args) throws Exception {
    for ( int i=0; i<args.length; i++ ) {
      LogEntry entry = logEntryFromString( args[i] );
      System.out.println( entry );
    }
  }


}
