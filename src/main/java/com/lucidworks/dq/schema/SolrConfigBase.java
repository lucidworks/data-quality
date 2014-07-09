package com.lucidworks.dq.schema;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;

public abstract class SolrConfigBase implements SolrConfig {

  @Override
  public String generateReport() throws Exception {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    // Singular Values

    String version = getLuceneMatchVersion();
    out.println( "Lucene Match Version = " + version );
    String abort = getAbortOnConfigurationError();
    out.println( "Abort on config error = " + abort );
    
    // Complex Values

    Collection<String> handlers = getRequestHandlers();
    out.println();
    out.println( "Request Handlers and Classes:" );
    for ( String handler : handlers ) {
      out.println( "\t" + handler );      
    }

    String outStr = sw.toString();
    return outStr;
  }

}
