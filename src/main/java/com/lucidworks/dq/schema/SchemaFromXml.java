package com.lucidworks.dq.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
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
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SchemaFromXml extends SchemaBase implements Schema {
  // get from resources folder
  static String SCHEMA_FILE_NAME = "schema-481.xml";
  
  Document document;
  XPathFactory xpathFactory = XPathFactory.newInstance();
  private final String prefix = null;
  private final String name = "";

  // Note: Some of this code was copied from:
  // * Solr's IndexSchema.java
  // * Solr's Config.java


  public SchemaFromXml() throws ParserConfigurationException, IOException, SAXException {
	// this( SCHEMA_FILE_NAME );
    // URL schemaPath = this.getClass().getResource( SCHEMA_FILE_NAME );
    // init( schemaPath );
    init( (URL) null );
  }
  public SchemaFromXml( File schemaPath ) throws ParserConfigurationException, SAXException, IOException {
	  // URI uri = schemaPath.toURI();
	  // URL url = uri.toURL();
	  // init( url );
	  InputStream is = new FileInputStream( schemaPath );
	  init( is );
  }
  public SchemaFromXml( URL schemaPath ) throws ParserConfigurationException, IOException, SAXException {
	  init( schemaPath );
  }
  void init( URL schemaPath ) throws ParserConfigurationException, IOException, SAXException {
    if ( null==schemaPath ) {
      schemaPath = this.getClass().getClassLoader().getResource( SCHEMA_FILE_NAME );
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
 
  // Parts copied from Solr's IndexSchema .loadFields
  public Set<String> getAllSchemaFieldNames() throws XPathExpressionException {
    Set<String> out = new LinkedHashSet<>();
    XPath xpath = xpathFactory.newXPath();
    // /schema/fields/field | /schema/fields/dynamicField
    // | /schema/field | /schema/dynamicField
    // Note: could remove OR and eliminate node name check, but this is closer to Solr code
    String expression = stepsToPath(SCHEMA, FIELDS, FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, FIELDS, DYNAMIC_FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, DYNAMIC_FIELD)
           ;
    NodeList nodes = (NodeList)xpath.evaluate(expression, document, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      NamedNodeMap attrs = node.getAttributes();
      String name = getAttr(attrs, NAME, "field definition");
      // String type = getAttr(attrs, TYPE, "field " + name);
      // TODO: lots more properties
      if (node.getNodeName().equals(FIELD)) {
    	  out.add( name );
      }
    }	
    return out;
  }
  // Parts copied from Solr's IndexSchema .loadFields
  public Set<String> getAllDynamicFieldPatterns() throws XPathExpressionException {
    Set<String> out = new LinkedHashSet<>();
    XPath xpath = xpathFactory.newXPath();
    // Note: could remove OR and eliminate node name check, but this is closer to Solr code
    // /schema/fields/field | /schema/fields/dynamicField
    String expression = stepsToPath(SCHEMA, FIELDS, FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, FIELDS, DYNAMIC_FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, FIELD)
           + XPATH_OR + stepsToPath(SCHEMA, DYNAMIC_FIELD)
           ;
    NodeList nodes = (NodeList)xpath.evaluate(expression, document, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      NamedNodeMap attrs = node.getAttributes();
      String name = getAttr(attrs, NAME, "field definition");
      // String type = getAttr(attrs, TYPE, "field " + name);
      // TODO: lots more properties
      if (node.getNodeName().equals(DYNAMIC_FIELD)) {
    	  out.add( name );
    	  // TODO: additional field checks for dynamic fields
      }
    }	
    return out;	  
  }
  public Set<String> getAllFieldTypeNames() throws XPathExpressionException {
    Set<String> out = new LinkedHashSet<>();
    XPath xpath = xpathFactory.newXPath();
    // "/schema/types/fieldtype | /schema/types/fieldType"
    String expression = stepsToPath(SCHEMA, TYPES, FIELD_TYPE.toLowerCase(Locale.ROOT))
          + XPATH_OR + stepsToPath(SCHEMA, TYPES, FIELD_TYPE)
          + XPATH_OR + stepsToPath(SCHEMA, FIELD_TYPE.toLowerCase(Locale.ROOT))
          + XPATH_OR + stepsToPath(SCHEMA, FIELD_TYPE)
          ;
    NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
    for( int i = 0; i < nodes.getLength(); i++ ) {
      Node node = nodes.item(i);
      // String name = node.getNodeValue();
      // String name = node.
      NamedNodeMap attrs = node.getAttributes();
      Node attrNode = attrs.getNamedItem( NAME );
      String name = attrNode.getTextContent();
      out.add( name );
    }
    return out;
  }
  Map<String,Set<String>> getAllCopyFieldsDictionary() throws XPathExpressionException {
    Map<String,Set<String>> out = new LinkedHashMap<>();
    XPath xpath = xpathFactory.newXPath();
    String expression = "//" + COPY_FIELD;
    NodeList nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      NamedNodeMap attrs = node.getAttributes();      
      String source = getAttrOrNull(attrs, SOURCE, COPY_FIELD + " definition");
      String dest   = getAttrOrNull(attrs, DESTINATION,  COPY_FIELD + " definition");
      // TODO: maxCharsInt
      // TODO: I think copyField can have duplicate source/destination pairs
      // TODO: addtional check: entry.getValue() > 1 && !entry.getKey().multiValued()
      if ( null!=source && null!=dest ) {
    	  if ( out.containsKey(source) ) {
    		  out.get( source ).add( dest );
    	  }
    	  else {
    		  Set<String> destList = new LinkedHashSet<>();
    		  destList.add( dest );
    		  out.put( source, destList );
    	  }
      }
    }
    return out;
  }
  Map<String,Set<String>> invertDictionary( Map<String,Set<String>> in ) {
    Map<String,Set<String>> out = new LinkedHashMap<>();
    for ( Entry<String, Set<String>> pair : in.entrySet() ) {
      String oldSource = pair.getKey();                 // AKA: newDest
      Set<String> oldDestList = pair.getValue(); // AKA: newSourceList
      // for newSource in newSourceList
      for ( String oldDest : oldDestList ) {
    	if ( out.containsKey(oldDest) ) {
    	  // newSource .add newDest
          out.get( oldDest ).add( oldSource );
        }
    	  else {
    	    Set<String> newDestList = new LinkedHashSet<>();
    	    newDestList.add( oldSource );
    	    out.put( oldDest, newDestList );
    	  }
      }
    }
    return out;
  }

  public Set<String> getAllCopyFieldSourceNames() throws XPathExpressionException {
    Map<String,Set<String>> copyFields = getAllCopyFieldsDictionary();
    return copyFields.keySet();
  }
  public Set<String> getAllCopyFieldDestinationNames() throws XPathExpressionException {
    Map<String,Set<String>> copyFields = getAllCopyFieldsDictionary();
    // Slightly less efficient to copy entire dict, but benefit is only 1 code path
    Map<String,Set<String>> invertedHash = invertDictionary( copyFields );
    return invertedHash.keySet(); 
//	Set<String> out = new LinkedHashSet<>();
//	for ( Entry<String, Collection<String>> pair : copyFields.entrySet() ) {
//      String source = pair.getKey();
//      Collection<String> destList = pair.getValue();
//      for ( String dest : destList ) {
//    	  out.add( dest );
//      }
//	}
//  return out;
  }
  public Set<String> getCopyFieldDestinationsForSource( String sourceName ) throws XPathExpressionException {
    Map<String,Set<String>> copyFields = getAllCopyFieldsDictionary();
    if ( copyFields.containsKey(sourceName) ) {
      return copyFields.get(sourceName);
    }
    else {
      return new LinkedHashSet<>();
    }
  }
  public Set<String> getCopyFieldSourcesForDestination( String destName ) throws XPathExpressionException {
    Map<String,Set<String>> copyFields = getAllCopyFieldsDictionary();
    Map<String,Set<String>> invertedHash = invertDictionary( copyFields );
    if ( invertedHash.containsKey(destName) ) {
      return invertedHash.get(destName);
    }
    else {
      return new LinkedHashSet<>();
    }
  }
  public float getSchemaVersion() throws Exception {
    // "/schema/@version"
    String expression = stepsToPath(SCHEMA, AT + VERSION);
    float version = /*schemaConf.*/ getFloat(expression, 1.0f);
    return version;
  }
  public String getSchemaName() throws Exception {
    XPath xpath = xpathFactory.newXPath();
    String expression = stepsToPath(SCHEMA, AT + NAME);
    Node nd = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    // In real Solr they have a fallback where you can get the name from
    // loader.getCoreProperties().getProperty(SOLR_CORE_NAME) where
    // SOLR_CORE_NAME = "solr.core.name"
    String name = null;
    if ( null != nd ) {
      name = nd.getNodeValue();
    }
    return name;
  }
  public String getUniqueKeyFieldName() throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    // /schema/uniqueKey/text()
    String expression = stepsToPath(SCHEMA, UNIQUE_KEY, TEXT_FUNCTION);
    Node node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String uniqueKeyField = null;
    if ( null!=node ) {
      uniqueKeyField = node.getNodeValue().trim();
    }
    return uniqueKeyField;
    // TODO: other things to check
    // SchemaField uniqueKeyField
    // uniqueKeyField=getIndexedField(node.getNodeValue().trim());
    // ! uniqueKeyField.stored()
    // uniqueKeyField.multiValued()
    // uniqueKeyFieldName=uniqueKeyField.getName();
    // uniqueKeyFieldType=uniqueKeyField.getType();
    // Boolean.FALSE != explicitRequiredProp.get( uniqueKeyFieldName )
  }
  public String getDefaultOperator() throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    //  /schema/solrQueryParser/@defaultOperator
    String expression = stepsToPath(SCHEMA, SOLR_QUERY_PARSER, AT + DEFAULT_OPERATOR);
    Node node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String queryParserDefaultOperator = null;
    if ( null!=node ) {
      queryParserDefaultOperator = node.getNodeValue().trim();
    }
    return queryParserDefaultOperator;
  }
  public String getSimilarityModelClassName() throws XPathExpressionException {
    // Default: org.apache.solr.search.similarities.DefaultSimilarityFactory
    XPath xpath = xpathFactory.newXPath();
    // /schema/similarity
    String expression = stepsToPath(SCHEMA, SIMILARITY);
    Node nd = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String name = null;
    if ( null!=nd ) {
    	//name = nd.getNodeValue();
      name = nd.getTextContent();
    }
    if ( null==name || name.trim().equals("") ) {
    	name = "org.apache.solr.search.similarities.DefaultSimilarityFactory";
    }
    return name;
  }
  public String getDefaultSearchField() throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    //  /schema/defaultSearchField/@text()
    String expression = stepsToPath(SCHEMA, DEFAULT_SEARCH_FIELD, TEXT_FUNCTION);
    Node node = (Node) xpath.evaluate(expression, document, XPathConstants.NODE);
    String defaultSearchFieldName = null;
    if ( null != node ) {
        defaultSearchFieldName = node.getNodeValue().trim();
    }
    return defaultSearchFieldName;
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
    SchemaFromXml s = new SchemaFromXml();

    float version = s.getSchemaVersion();
    System.out.println( "Version = " + version );
    String name = s.getSchemaName();
    System.out.println( "Schema Name: " + name );
    String key = s.getUniqueKeyFieldName();
    System.out.println( "Key Field: " + key );
    String defOp = s.getDefaultOperator();
    System.out.println( "Default Operator: " + defOp );
    String sim = s.getSimilarityModelClassName();
    System.out.println( "Similarity Class Name: " + sim );
    String defField = s.getDefaultSearchField();
    System.out.println( "Default Search Field: " + defField );

    Set<String> fields = s.getAllSchemaFieldNames();
    System.out.println( "Fields: " + fields );

    Set<String> dynFields = s.getAllDynamicFieldPatterns();
    System.out.println( "Dynamic field Patterns: " + dynFields );

    Set<String> typeNames = s.getAllFieldTypeNames();
    System.out.println( "Types: " + typeNames );
    
    Set<String> sourceNames = s.getAllCopyFieldSourceNames();
    System.out.println( "Copy Sources: " + sourceNames );
    for ( String source : sourceNames ) {
    	Set<String> tmpDests = s.getCopyFieldDestinationsForSource(source);
    	System.out.println( "\tFrom: '"+ source + "' To " + tmpDests );
    }

    Set<String> destNames = s.getAllCopyFieldDestinationNames();
    System.out.println( "Copy Destinations: " + destNames );
    for ( String dest : destNames ) {
    	Set<String> tmpSrcs = s.getCopyFieldSourcesForDestination( dest );
    	System.out.println( "\tDest: '"+ dest + "' From " + tmpSrcs );
    }

  }
  
  // Copied from Solr's IndexSchema.java
  public static final String COPY_FIELD = "copyField";
  public static final String DEFAULT_OPERATOR = "defaultOperator";
  public static final String DEFAULT_SEARCH_FIELD = "defaultSearchField";
  public static final String DESTINATION = "dest";
  public static final String DYNAMIC_FIELD = "dynamicField";
  public static final String FIELD = "field";
  public static final String FIELDS = FIELD + "s";
  public static final String FIELD_TYPE = "fieldType";
  public static final String NAME = "name";
  public static final String SCHEMA = "schema";
  public static final String SIMILARITY = "similarity";
  public static final String SOURCE = "source";
  public static final String SLASH = "/";
  public static final String SOLR_QUERY_PARSER = "solrQueryParser";
  public static final String TYPE = "type";
  public static final String TYPES = "types";
  public static final String UNIQUE_KEY = "uniqueKey";
  public static final String VERSION = "version";

  private static final String AT = "@";
  private static final String TEXT_FUNCTION = "text()";
  private static final String XPATH_OR = " | ";

}