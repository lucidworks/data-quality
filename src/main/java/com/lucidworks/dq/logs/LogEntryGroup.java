package com.lucidworks.dq.logs;

import java.util.Collection;

/*
 * TODO: Do we really need this?
 * Pro: good abstraction, might developer additional features
 * Con: converting back and forth between this and Collection<LogEntry>
 */
public interface LogEntryGroup /*extends Collection<LogEntry>*/ {
  Collection<LogEntry> getEntries();
}
