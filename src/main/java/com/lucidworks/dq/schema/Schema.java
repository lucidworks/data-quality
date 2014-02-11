package com.lucidworks.dq.schema;

import java.util.Set;

public interface Schema {

	// TODO: move throws Exception down to implementation level
	// and errors buffer

	public float getSchemaVersion() throws Exception;

	public String getSchemaName() throws Exception;

	public String getUniqueKeyFieldName() throws Exception;

	public String getSimilarityModelClassName() throws Exception;

	public String getDefaultOperator() throws Exception;

	public String getDefaultSearchField() throws Exception;

	public Set<String> getAllSchemaFieldNames() throws Exception;

	public Set<String> getAllDynamicFieldPatterns() throws Exception;

	public Set<String> getAllFieldTypeNames() throws Exception;

	public Set<String> getAllCopyFieldSourceNames() throws Exception;

	public Set<String> getAllCopyFieldDestinationNames() throws Exception;

	public Set<String> getCopyFieldDestinationsForSource(String sourceName) throws Exception;

	public Set<String> getCopyFieldSourcesForDestination(String destName) throws Exception;

	public String generateReport() throws Exception;

}