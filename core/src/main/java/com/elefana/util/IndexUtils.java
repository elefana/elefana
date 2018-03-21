/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.util;

import java.util.List;

import com.elefana.api.exception.ElefanaException;

public interface IndexUtils {
	public static final String DATA_TABLE = "elefana_data";
	public static final String PARTITION_TRACKING_TABLE = "elefana_partition_tracking";

	public static final String TRIGGERS_PREFIX = "elefana_triggers_";
	public static final String GIN_INDEX_PREFIX = "elefana_gin_idx_";
	public static final String JSON_INDEX_PREFIX = "elefana_json_idx_";
	public static final String PRIMARY_KEY_PREFIX = "elefana_pkey_";
	
	public String generateDocumentId(String index, String type, String source);

	public List<String> listIndices() throws ElefanaException;

	public List<String> listIndicesForIndexPattern(List<String> indexPatterns) throws ElefanaException;

	public List<String> listIndicesForIndexPattern(String indexPattern) throws ElefanaException;

	public String getQueryTarget(String indexName);
	
	public long getTimestamp(String index, String document) throws ElefanaException;

	public void ensureIndexExists(String indexName) throws ElefanaException;
	
	public void ensureJsonFieldIndexExist(String indexName, List<String> fieldNames) throws ElefanaException;

	public void deleteIndex(String indexName);
	
	public void deleteTemporaryTable(String tableName);

	public String getIndexForPartitionTable(String partitionTable);

	public String getPartitionTableForIndex(String index);

	public static String destringifyJson(String json) {
		if (json.startsWith("\"")) {
			json = json.substring(1, json.length() - 1);
			json = json.replace("\\", "");
		}
		return json;
	}

	public static boolean isTypesEmpty(String[] types) {
		if (types == null) {
			return true;
		}
		if (types.length == 0) {
			return true;
		}
		for (int i = 0; i < types.length; i++) {
			if (types[i] == null) {
				continue;
			}
			if (types[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}
	
	/**
	 * If JSON string contains \" we need to escape it as \\" for PSQL to handle correctly
	 * @param json The original JSON string
	 * @return The escaped JSON string
	 */
	public static String psqlEscapeString(String json) {
		return json.replace("\\\"", "\\\\\"");
	}
}
