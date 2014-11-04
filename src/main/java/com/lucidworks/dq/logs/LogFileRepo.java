package com.lucidworks.dq.logs;

import java.io.File;
import java.util.Collection;

public interface LogFileRepo {
  // TODO: handle streaming, via socket, and one-at-a-time iteration
  // Actually these could be considered *candidate* log files...
  //Collection<LogFile> findLogFiles( File startingDirOrFile );
  //Collection<LogFile> findLogFiles( Collection<File> startingDirOrFiles );
  Collection<LogFile> findLogFiles();

  // TODO: maybe Log *File* Repo is a filesystem impl of a more generic Log Unit Source Repo
  // TODO: although we really do need setters, should they be defined in the interface?
  String getIncludePattern();
  void setIncludePattern( String pattern );
  boolean getIncludeCompressedFiles();
  void setIncludeCompressedFiles( boolean flag );
}
