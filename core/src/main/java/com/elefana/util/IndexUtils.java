/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.util;

import java.util.List;

import com.elefana.api.exception.ElefanaException;

public interface IndexUtils {
	public static final String DATA_TABLE = "elefana_data";
	public static final String PARTITION_TRACKING_TABLE = "elefana_partition_tracking";

	public static final String TRIGGERS_PREFIX = "elefana_triggers_";
	public static final String GIN_INDEX_PREFIX = "elefana_gin_idx_";
	public static final String TIMESTAMP_INDEX_PREFIX = "elefana_ts_idx_";
	public static final String SECOND_INDEX_PREFIX = "elefana_1s_idx_";
	public static final String MINUTE_INDEX_PREFIX = "elefana_1m_idx_";
	public static final String HOUR_INDEX_PREFIX = "elefana_1h_idx_";
	public static final String DAY_INDEX_PREFIX = "elefana_1d_idx_";
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
		if(!json.contains("\\\"")) {
			return json;
		}
		for(int i = 0; i < json.length() - 1; i++) {
			if(json.charAt(i) != '\\') {
				continue;
			}
			switch(json.charAt(i + 1)) {
			case '\\':
				if(i + 2 >= json.length()) {
					continue;
				}
				switch(json.charAt(i + 2)) {
				case '\\':
					if(i + 3 >= json.length()) {
						continue;
					}
					switch(json.charAt(i + 3)) {
					case '\"':
						i += 3;
						continue;
					}
					break;
				}
				break;
			case '\"':
				json = json.substring(0, i) + "\\\\\\\"" + json.substring(i + 2);
				i += 3;
				break;
			default:
				continue;
			}
		}
		return json;
	}
	
	public static String psqlUnescapeString(String json) {
		if(!json.contains("\\\"")) {
			return json;
		}
		for(int i = 0; i < json.length() - 1; i++) {
			if(json.charAt(i) != '\\') {
				continue;
			}
			switch(json.charAt(i + 1)) {
			case '\\':
				if(i + 2 >= json.length()) {
					continue;
				}
				switch(json.charAt(i + 2)) {
				case '\\':
					if(i + 3 >= json.length()) {
						continue;
					}
					switch(json.charAt(i + 3)) {
					case '\"':
						json = json.substring(0, i) + json.substring(i + 2);
						continue;
					}
					break;
				}
				break;
			default:
				continue;
			}
		}
		return json;
	}
}
