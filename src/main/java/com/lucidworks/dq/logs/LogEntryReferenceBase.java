package com.lucidworks.dq.logs;

import java.util.ArrayList;
import java.util.Collection;

public class LogEntryReferenceBase implements LogEntryReference {

  String comment;
  // LogEntryGroup is approx Collection<LogEntry>
  Collection<LogEntry> earlierEntries = new ArrayList<>();
  Collection<LogEntry> laterEntries = new ArrayList<>();

  int relativeRegionOfInterestStart;
  int relativeRegionOfInterestEnd;
  
  public LogEntryReferenceBase() { }
  
  public LogEntryReferenceBase( LogEntry earlierEntry, LogEntry laterEntry, String comment ) {
    this();
    // Link to log entries
    addEarlierEntry( earlierEntry );
    addLaterEntry( laterEntry );
    // Link log entries back to us
    ( (LogEntryBase)earlierEntry ).addReference( this );
    ( (LogEntryBase)laterEntry ).addReference( this );
    setComment( comment );
  }

  @Override
  public Collection<LogEntry> getEarlierEntries() {
    return earlierEntries;
  }
  public void addEarlierEntry( LogEntry entry ) {
    earlierEntries.add( entry );
  }

  @Override
  public Collection<LogEntry> getLaterEntries() {
    return laterEntries;
  }
  public void addLaterEntry( LogEntry entry ) {
    laterEntries.add( entry );
  }

  @Override
  public String getComment() {
    return comment;
  }
  public void setComment( String comment ) {
    this.comment = comment;
  }

  @Override
  public int getRelativeStart() {
    return relativeRegionOfInterestStart;
  }
  @Override
  public int getRelativeEnd() {
    return relativeRegionOfInterestEnd;
  }
  //@Override
  public void setRelativeRegionOfInterest( int fromStart, int fromEnd ) {
    relativeRegionOfInterestStart = fromStart;
    relativeRegionOfInterestEnd = fromEnd;
  }
  //@Override
  public void setRelativeStart( int fromStart ) {
    this.relativeRegionOfInterestStart = fromStart;
  }
  //@Override
  public void setRelativeEnd( int fromEnd ) {
    this.relativeRegionOfInterestEnd = fromEnd;
  }
}
