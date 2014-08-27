package com.lucidworks.dq.schema;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public abstract class SchemaBase implements Schema {

  // Also helpful for debugging code
  @Override
  public String generateReport() throws Exception {
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);

    // Singular Values
    String name = getSchemaName();
    out.println( "Schema Name: " + name );
    float vers = getSchemaVersion();
    out.println("Schema Version: " + vers);
    String key = getUniqueKeyFieldName();
    out.println( "Key Field: " + key );
    String defOp = getDefaultOperator();
    out.println( "Default Operator: " + defOp );
    String sim = getSimilarityModelClassName();
    out.println( "Similarity Class Name: " + sim );
    String defField = getDefaultSearchField();
    out.println( "Default Search Field: " + defField );

    // Complex Values
    Set<String> fields = getAllSchemaFieldNames();
    out.println();
    out.println( "Fields: " + fields );

    Set<String> dynFields = getAllDynamicFieldPatterns();
    out.println();
    out.println( "Dynamic Field Patterns: " + dynFields );

    Set<String> typeNames = getAllFieldTypeNames();
    out.println();
    out.println( "Types: " + typeNames );

    Map<String, Set<String>> typesAndNames = getAllDeclaredAndDynamicFieldsByType();
    out.println();
    out.println( "Type -> Fields: (declared and dynamic patterns)" );
    out.println( "\t(" + typesAndNames.size() + " types)" );
    for ( String type : typesAndNames.keySet() ) {
      out.println( "\t" + type + ":" );
      Set<String> typeFields = typesAndNames.get( type );
      out.println( "\t\t(" + typeFields.size() + " fields)" );
      for ( String field : typeFields ) {
        out.println( "\t\t" + field );        
      }
    }
    
    
    Set<String> sourceNames = getAllCopyFieldSourceNames();
    out.println();
    out.println( "Copy Sources: " + sourceNames );
    for ( String source : sourceNames ) {
      Set<String> tmpDests = getCopyFieldDestinationsForSource(source);
      out.println( "\tFrom: '"+ source + "' To " + tmpDests );
    }

    Set<String> destNames = getAllCopyFieldDestinationNames();
    out.println();
    out.println( "Copy Destinations: " + destNames );
    for ( String dest : destNames ) {
      Set<String> tmpSrcs = getCopyFieldSourcesForDestination( dest );
      out.println( "\tDest: '"+ dest + "' From " + tmpSrcs );
    }

    String outStr = sw.toString();
    return outStr;
  }
  
  static void utilTabulateFieldTypeAndName( Map<String, Set<String>> map, String type, String name ) {
    if ( map.containsKey(type) ) {
      map.get(type).add( name );
    }
    else {
      Set<String> vector = new LinkedHashSet<>();
      vector.add( name );
      map.put( type, vector );
    }   
  }

  @Override
  public abstract float getSchemaVersion() throws Exception;
  @Override
  public abstract String getSchemaName() throws Exception;
  @Override
  public abstract String getUniqueKeyFieldName() throws Exception;
  @Override
  public abstract String getSimilarityModelClassName() throws Exception;
  @Override
  public abstract String getDefaultOperator() throws Exception;
  @Override
  public abstract String getDefaultSearchField() throws Exception;
  @Override
  public abstract Set<String> getAllSchemaFieldNames() throws Exception;
  @Override
  public abstract Set<String> getAllDynamicFieldPatterns() throws Exception;
  @Override
  public abstract Set<String> getAllFieldTypeNames() throws Exception;
  @Override
  public abstract Set<String> getAllCopyFieldSourceNames() throws Exception;
  @Override
  public abstract Set<String> getAllCopyFieldDestinationNames() throws Exception;
  @Override
  public abstract Set<String> getCopyFieldDestinationsForSource(String sourceName) throws Exception;
  @Override
  public abstract Set<String> getCopyFieldSourcesForDestination(String destName) throws Exception;

}
