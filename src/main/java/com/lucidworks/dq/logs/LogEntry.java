package com.lucidworks.dq.logs;

import java.util.Collection;

/*
 * Log entries can have structure.
 * Sometimes the structure isn't known when log entries are first ingested, they may come in as raw strings.
 * The idea is that a log entry could be fed into a process and then a more specific log entry comes out.
 * This process could be repeated for even more specific or normalized entries.
 * Ideally more evolved log entries can have the option of still referring back to their parent entries
 * for auditing or so that rules can be rerun.
 * Another issue is that some series of lines in a log file constitute a higher level log entry.
 * Some of the strecture might be fixed text, whereas other items might be parameterizable.
 * Eg:
 *  &name=dave
 *  &name=mark
 *  &name=satish
 *  -> "name" is a fixed identifier, whereas values can vary.
 *  
 *  My post on Stack Overflow:
 *  http://stackoverflow.com/questions/26518770/advanced-requirements-for-log-file-utilities-am-i-reinventing-the-wheel
 */
interface LogEntry {

  String getRawText();
  
  Collection<LogEntryReference> getReferences();
  // TODO: should setters be defined in Interface?
  // void addReference( LogEntryReference ref );

  // getDate
  // getPath
  // getHandler
  // getParamsString
  // getParent
  // getChildren
  // getEntities
  // getEventLevel // Info, warn, error, default
  
}
