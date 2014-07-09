package com.lucidworks.dq.schema;

import java.util.Collection;

import javax.xml.xpath.XPathExpressionException;

public interface SolrConfig {

  public String generateReport() throws Exception;

  // Can't return float, could be const or config
  public String getLuceneMatchVersion() throws Exception;

  // Can't return bool, could be const or config
  public String getAbortOnConfigurationError() throws Exception;

  public Collection<String> getRequestHandlers() throws Exception;

}