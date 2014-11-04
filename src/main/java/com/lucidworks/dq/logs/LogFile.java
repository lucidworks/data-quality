package com.lucidworks.dq.logs;

import java.io.IOException;
import java.util.Collection;

public interface LogFile extends LogEntryGroup {

  void read() throws IOException;
  
  // Inherits getEntries() from super

}
