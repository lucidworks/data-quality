package com.lucidworks.dq.logs;

import java.util.Collection;

public interface LogEntryReference {
  Collection<LogEntry> getEarlierEntries();
  Collection<LogEntry> getLaterEntries();
  //void addEarlierEntry( LogEntry entry );
  //void addLaterEntry( LogEntry entry );

  String getComment();
  //void setComment( String comment );

  int getRelativeStart();
  int getRelativeEnd();
  //void setRelativeRegionOfInterest( int fromStart, int fromEnd );
  //void setRelativeStart( int fromStart );
  //void setRelativeEnd( int fromEnd );
}
