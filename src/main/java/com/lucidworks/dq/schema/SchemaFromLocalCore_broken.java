package com.lucidworks.dq.schema;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.ConfigSolrXmlOld;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchema.DynamicField;
import org.apache.solr.schema.SchemaField;

public class SchemaFromLocalCore_broken extends SchemaBase implements Schema {

  static String PATH1 = "/Users/mbennett/data/dev/solr-lucene-461-src/solr/example";
  static String PATH2 = "/Users/mbennett/data/dev/solr-lucene-461-src/solr/example/solr";
  static String PATH3 = "/Users/mbennett/data/dev/solr-lucene-461-src/solr/example/solr/collection1";

  private IndexSchema schema;
  
  public SchemaFromLocalCore_broken( String path, String optCoreName ) {
    // TODO: currently broken, touble finding info online, postponing for now
    SolrResourceLoader loader = new SolrResourceLoader( path );
    String confDir = loader.getConfigDir();
    String dataDir = loader.getDataDir();
    String instanceDir = loader.getInstanceDir();
    Properties props = loader.getCoreProperties();
    System.out.println( "path = " + path );
    System.out.println( "confDir = " + confDir );
    System.out.println( "dataDir = " + dataDir );
    System.out.println( "instanceDir = " + instanceDir );
    System.out.println( "props = " + props );
    ConfigSolr config = ConfigSolr.fromSolrHome( loader, path );
    CoreContainer container = new CoreContainer( loader, config );
    if ( container.getCores().isEmpty() ) {
      throw new IllegalArgumentException( "No cores found at " + path );
    }
    String coreName = optCoreName!=null ? optCoreName : ConfigSolrXmlOld.DEFAULT_DEFAULT_CORE_NAME;
    SolrCore core = container.getCore( coreName );
    if ( null==core ) {
      throw new IllegalArgumentException( "Unable to find core \"" + coreName + "\" at " + path );
    }
    // SolrQueryRequest req = new LocalSolrQueryRequest( core, "*:*", null, 0, 0, null );
    NamedList args = new NamedList();
    SolrQueryRequest req = new LocalSolrQueryRequest( core, args );
    schema = req.getSchema();
  };

  public float getSchemaVersion() throws Exception {
    return schema.getVersion();
  }

  public String getSchemaName() throws Exception {
    return schema.getSchemaName();
  }

  public String getUniqueKeyFieldName() throws Exception {
    return schema.getUniqueKeyField().getName();
  }

  public String getSimilarityModelClassName() throws Exception {
    return schema.getSimilarity().getClass().getName();
  }

  // TODO: not sure where this comes from
  public String getDefaultOperator() throws Exception {
    return null;
  }

  public String getDefaultSearchField() throws Exception {
    return schema.getDefaultSearchFieldName();
  }

  public Set<String> getAllSchemaFieldNames() throws Exception {
    Map<String, SchemaField> fields = schema.getFields();
    return fields.keySet();
    // return new LinkedHashSet<>( fields.keySet() );
  }

  public Set<String> getAllDynamicFieldPatterns() throws Exception {
    DynamicField[] dynFields = schema.getDynamicFields();
    Set<String> out = new LinkedHashSet<>();
    for ( DynamicField df : dynFields ) {
      out.add( df.getRegex() );
    }
    return out;
  }

  public Set<String> getAllFieldTypeNames() throws Exception {
    Map<String, FieldType> types = schema.getFieldTypes();
    return types.keySet();
  }

  public Set<String> getAllCopyFieldSourceNames() throws Exception {
    Map<String, List<CopyField>> copyMap = schema.getCopyFieldsMap();
    return copyMap.keySet();
  }

  public Set<String> getAllCopyFieldDestinationNames() throws Exception {
    Set<String> out = new LinkedHashSet<>();
    Map<String, List<CopyField>> copyMap = schema.getCopyFieldsMap();
    for ( Entry<String, List<CopyField>> copyEntry : copyMap.entrySet() ) {
      // String srcFieldName = copyEntry.getKey();
      List<CopyField> copyList = copyEntry.getValue();
      for ( CopyField cf : copyList ) {
        SchemaField destField = cf.getDestination();
        out.add( destField.getName() );
      }
    }
    return out;
  }

  public Set<String> getCopyFieldDestinationsForSource(String sourceName) throws Exception {
    Set<String> out = new LinkedHashSet<>();
    List<CopyField> copyList = schema.getCopyFieldsList( sourceName );
    if ( null==copyList || copyList.isEmpty() ) {
      return out;
    }
    for ( CopyField cf : copyList ) {
      SchemaField destField = cf.getDestination();
      out.add( destField.getName() );
    }
    return out;
  }

  public Set<String> getCopyFieldSourcesForDestination(String targetDestName) throws Exception {
    Set<String> out = new LinkedHashSet<>();
    Map<String, List<CopyField>> copyMap = schema.getCopyFieldsMap();
    for ( Entry<String, List<CopyField>> copyEntry : copyMap.entrySet() ) {
      String srcFieldName = copyEntry.getKey();
      List<CopyField> copyList = copyEntry.getValue();
      for ( CopyField cf : copyList ) {
        SchemaField destField = cf.getDestination();
        String destFieldName = destField.getName();
        if ( destFieldName.equals(targetDestName) ) {
          out.add( srcFieldName );
        }
      }
    }
    return out;
  }

  // public String generateReport() throws Exception;

  public static void main( String[] argv ) throws Exception {
    Schema schema = new SchemaFromLocalCore_broken( PATH3, null );
    schema.generateReport();
  }
}