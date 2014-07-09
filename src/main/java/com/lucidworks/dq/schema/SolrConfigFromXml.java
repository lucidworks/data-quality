package com.lucidworks.dq.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Node;

public class SolrConfigFromXml extends SolrConfigBase implements SolrConfig {
  // get from resources folder
  static String CONFIG_FILE_NAME = "solrconfig-480.xml";
  
  Document document;
  XPathFactory xpathFactory = XPathFactory.newInstance();
  private final String prefix = null;
  private final String name = "";

  // Note: Some of this code was copied from:
  // * Solr's IndexSchema.java
  // * Solr's Config.java


  public SolrConfigFromXml() throws ParserConfigurationException, IOException, SAXException {
    // this( SCHEMA_FILE_NAME );
    //URL schemaPath = this.getClass().getResource( CONFIG_FILE_NAME );
    //init( schemaPath );
    init( (URL) null );
  }
  public SolrConfigFromXml( File schemaPath ) throws ParserConfigurationException, SAXException, IOException {
    // URI uri = schemaPath.toURI();
    // URL url = uri.toURL();
    // init( url );
    InputStream is = new FileInputStream( schemaPath );
    init( is );
  }
  public SolrConfigFromXml( URL schemaPath ) throws ParserConfigurationException, IOException, SAXException {
    init( schemaPath );
  }
  void init( URL schemaPath ) throws ParserConfigurationException, IOException, SAXException {
    if ( null==schemaPath ) {
      schemaPath = this.getClass().getClassLoader().getResource( CONFIG_FILE_NAME );
    }
    InputStream is = schemaPath.openConnection().getInputStream();
    init( is );
  }
  void init( InputStream in ) throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    this.document = builder.parse( in );
    xpathFactory = XPathFactory.newInstance();
  }

  // Can't return float, could be const or config
  /* (non-Javadoc)
   * @see com.lucidworks.dq.schema.SolrConfig#getLuceneMatchVersion()
   */
  @Override
  public String getLuceneMatchVersion() throws Exception {
    XPath xpath = xpathFactory.newXPath();
    // "/config/luceneMatchVersion"
    String expression = stepsToPath(CONFIG, LUCENE_VERSION);
    // float version = getFloat(expression, 0.0f);
    Node nd = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String payload = null;
    if ( null!=nd ) {
      // payload = nd.getNodeValue();
      payload = nd.getTextContent();
    }
    return payload;
  }

  // Can't return bool, could be const or config
  /* (non-Javadoc)
   * @see com.lucidworks.dq.schema.SolrConfig#getAbortOnConfigurationError()
   */
  @Override
  public String getAbortOnConfigurationError() throws Exception {
    XPath xpath = xpathFactory.newXPath();
    // "/config/abortOnConfigurationError"
    String expression = stepsToPath(CONFIG, ABORT);
    Node nd = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String payload = null;
    if ( null!=nd ) {
      payload = nd.getTextContent();
    }
    return payload;
  }
  
  // TODO: getLibs: <lib dir="../../dist/" regex="apache-solr-dataimporthandler-\d.*\.jar" />
  // TODO: getDataDir: <dataDir>${solr.data.dir:}</dataDir>
  // TODO: getDirectoryFactory: <directoryFactory name="DirectoryFactory" class="${solr.directoryFactory:solr.StandardDirectoryFactory}"/>
  // TODO: getIndexConfig (nested!): <indexConfig>
  // TODO: <jmx />
  // TODO: <updateHandler class="solr.DirectUpdateHandler2">
  // TODO: nested <indexReaderFactory name="IndexReaderFactory" class="package.class">
  // TODO: Nested: <query>
  // TODO: Nested: <requestDispatcher>
  // * TODO: Request Handlers, Nested!
  //   <requestHandler name="/select" class="solr.SearchHandler">
  //   <requestHandler name="/browse" class="solr.SearchHandler">
  //   <requestHandler name="/update" class="solr.XmlUpdateRequestHandler">
  //   <requestHandler name="/update/javabin" class="solr.BinaryUpdateRequestHandler" />
  //   <requestHandler name="/update/csv" class="solr.CSVRequestHandler" startup="lazy" />
  //   <requestHandler name="/update/json" class="solr.JsonUpdateRequestHandler" startup="lazy" />
  //   <requestHandler name="/update/extract" startup="lazy" class="solr.extraction.ExtractingRequestHandler" >
  //   <requestHandler name="/update/xslt" startup="lazy" class="solr.XsltUpdateRequestHandler"/>
  // Parts copied from Solr's IndexSchema .loadFields
  /* (non-Javadoc)
   * @see com.lucidworks.dq.schema.SolrConfig#getRequestHandlers()
   */
  @Override
  public Collection<String> getRequestHandlers() throws XPathExpressionException {
    Collection<String> out = new ArrayList<>();
    XPath xpath = xpathFactory.newXPath();
    // /schema/fields/field | /schema/fields/dynamicField
    // | /schema/field | /schema/dynamicField
    // Note: could remove OR and eliminate node name check, but this is closer to Solr code
    String expression = stepsToPath(CONFIG, HANDLER);
    NodeList nodes = (NodeList)xpath.evaluate(expression, document, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      NamedNodeMap attrs = node.getAttributes();
      String name = getAttr(attrs, NAME, "handler_name" );
      String classStr = getAttr(attrs, CLASS, "class_name" );
      String key = "" + name + ":" + classStr;
      out.add( key );
    } 
    return out;
  }

  // from solr/core/src/java/org/apache/solr/schema/IndexSchema.java
  /**
   * Converts a sequence of path steps into a rooted path, by inserting slashes in front of each step.
   * @param steps The steps to join with slashes to form a path
   * @return a rooted path: a leading slash followed by the given steps joined with slashes
   * Copied from Solr's IndexSchema.java
   */
  private String stepsToPath(String... steps) {
    StringBuilder builder = new StringBuilder();
    for (String step : steps) { builder.append(SLASH).append(step); }
    return builder.toString();
  } 

  // Copied from Solr's Config.java
  float getFloat(String path) throws NumberFormatException, Exception {
    return Float.parseFloat(getVal(path, true));
  }
  // Copied from Solr's Config.java
  float getFloat(String path, float def) throws Exception {
    String val = getVal(path, false);
    return val!=null ? Float.parseFloat(val) : def;
  }
  String getVal(String path, boolean errIfMissing) throws Exception {
    Node nd = getNode(path,errIfMissing);
    if (nd==null) return null;

    // String txt = DOMUtil.getText(nd);
    // TODO: node DOM Level 2 compatible, see Solr's DOMUtil.getText
    String txt = nd.getTextContent();

    // log.debug(name + ' '+path+'='+txt);
    return txt;
  }
  // Copied from Solr's Config.java
  Node getNode(String path, boolean errifMissing) throws Exception {
    return getNode(path, document, errifMissing);
  }
  // Copied from Solr's Config.java
  Node getNode(String path, Document doc, boolean errIfMissing) throws Exception {
    XPath xpath = xpathFactory.newXPath();
    String xstr = normalize(path);

    try {
      NodeList nodes = (NodeList)xpath.evaluate(xstr, doc,
                                                XPathConstants.NODESET);
      if (nodes==null || 0 == nodes.getLength() ) {
        if (errIfMissing) {
          throw new RuntimeException(name + " missing "+path);
        } else {
          // log.debug(name + " missing optional " + path);
          return null;
        }
      }
      if ( 1 < nodes.getLength() ) {
        throw new /*Solr*/ Exception( /*SolrException.ErrorCode.SERVER_ERROR,*/
                                 name + " contains more than one value for config path: " + path);
      }
      Node nd = nodes.item(0);
      // log.trace(name + ":" + path + "=" + nd);
      return nd;

    } catch (XPathExpressionException e) {
      // SolrException.log(log,"Error in xpath",e);
      throw new /*Solr*/ Exception( /*SolrException.ErrorCode.SERVER_ERROR,*/"Error in xpath:" + xstr + " for " + name,e);
//    } catch (SolrException e) {
//      throw(e);
    } catch (Throwable e) {
      // SolrException.log(log,"Error in xpath",e);
      throw new /*Solr*/Exception( /*SolrException.ErrorCode.SERVER_ERROR,*/"Error in xpath:" + xstr+ " for " + name,e);
    }
  }
  // Copied from Solr's IndexSchema.java
  private String normalize(String path) {
    return (prefix==null || path.startsWith("/")) ? path : prefix+path;
  }
  // Copied from Solr's DOMUtil.java
  public static String getAttr(NamedNodeMap attrs, String name, String missing_err) {
    Node attr = attrs==null? null : attrs.getNamedItem(name);
    if (attr==null) {
      if (missing_err==null) return null;
      throw new RuntimeException(missing_err + ": missing mandatory attribute '" + name + "'");
    }
    String val = attr.getNodeValue();
    return val;
  }
  public static String getAttrOrNull(NamedNodeMap attrs, String name, String missing_err) {
    try {
      return getAttr( attrs, name, missing_err );
    }
    catch( RuntimeException e ) {
      System.err.println( e );
      return null;
    }
  }

  public static void main( String[] argv ) throws Exception {
    SolrConfig s = new SolrConfigFromXml();

    String version = s.getLuceneMatchVersion();
    System.out.println( "Lucene Match Version = " + version );
    String abort = s.getAbortOnConfigurationError();
    System.out.println( "Abort on config error = " + abort );
    
    Collection<String> handlers = s.getRequestHandlers();
    System.out.println( "Request Handlers:" );
    for ( String handler : handlers ) {
      System.out.println( "\t" + handler );      
    }

  }

  // Strings taken from SolrConfig.java
  // but many are defined inline
  public static final String ABORT = "abortOnConfigurationError";
  public static final String CLASS = "class";
  public static final String CONFIG = "config";
  public static final String _DEFAULT = "default";  // TODO: request hnadler, others?, not implemented yet
  public static final String HANDLER = "requestHandler";
  public static final String LUCENE_VERSION = "luceneMatchVersion"; 
  public static final String NAME = "name"; 
  public static final String SLASH = "/";
  public static final String _STARTUP = "startup";  // TODO: request hnadler, not implemented yet
}
