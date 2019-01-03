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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.elefana.api.exception.ElefanaException;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

public interface IndexUtils {
	public static final String DATA_TABLE = "elefana_data";
	public static final String PARTITION_TRACKING_TABLE = "elefana_partition_tracking";

	public static final String TRIGGERS_PREFIX = "elefana_triggers_";
	public static final String BTREE_INDEX_PREFIX = "elefana_btree_idx_";
	public static final String GIN_INDEX_PREFIX = "elefana_gin_idx_";
	public static final String BRIN_INDEX_PREFIX = "elefana_brin_idx_";
	public static final String PRIMARY_KEY_PREFIX = "elefana_pkey_";
	
	public String generateDocumentId(String index, String type, String source);

	public List<String> listIndices() throws ElefanaException;

	public List<String> listIndicesForIndexPattern(List<String> indexPatterns) throws ElefanaException;

	public List<String> listIndicesForIndexPattern(String indexPattern) throws ElefanaException;

	public String getQueryTarget(Connection connection, String indexName) throws SQLException;

	public String getQueryTarget(String indexName);
	
	public long getTimestamp(String index, String document) throws ElefanaException;

	public void ensureIndexExists(String indexName) throws ElefanaException;
	
	public void ensureJsonFieldIndexExist(String indexName, List<String> fieldNames) throws ElefanaException;

	public void deleteIndex(String indexName);
	
	public void deleteTemporaryTable(String tableName);

	public String getIndexForPartitionTable(Connection connection, String partitionTable) throws SQLException;

	public String getIndexForPartitionTable(String partitionTable);

	public String getPartitionTableForIndex(Connection connection, String index) throws SQLException;

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

	public static String flattenJson(String json) {
		final StringBuilder result = new StringBuilder();
		final Any root = JsonIterator.deserialize(json);
		result.append('{');
		flattenJson("", root, result);
		result.append('}');
		return result.toString();
	}

	public static void flattenJson(String prefix, Any obj, StringBuilder stringBuilder) {
		final Any.EntryIterator iterator = obj.entries();

		boolean appendedField = false;
		while(iterator.next()) {
			if(appendedField) {
				stringBuilder.append(',');
			}
			switch(iterator.value().valueType()) {
			case INVALID:
				break;
			case STRING:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + iterator.key());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append('\"');
				stringBuilder.append(iterator.value().toString());
				stringBuilder.append('\"');
				appendedField = true;
				break;
			case NUMBER:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + iterator.key());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(iterator.value().toString().trim());
				appendedField = true;
				break;
			case NULL:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + iterator.key());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append("null");
				appendedField = true;
				break;
			case BOOLEAN:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + iterator.key());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(iterator.value().toBoolean());
				appendedField = true;
				break;
			case ARRAY:
				final List<Any> list = iterator.value().asList();
				flattenJson(prefix + iterator.key(), list, stringBuilder);
				appendedField = true;
				break;
			case OBJECT:
				flattenJson(prefix + iterator.key() + "_", iterator.value(), stringBuilder);
				appendedField = true;
				break;
			}
		}
	}

	public static void flattenJson(String prefix, List<Any> list, StringBuilder stringBuilder) {
		for(int i = 0; i < list.size(); i++) {
			switch(list.get(i).valueType()) {
			case INVALID:
				break;
			case STRING:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + "_" + i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append('\"');
				stringBuilder.append(list.get(i).toString());
				stringBuilder.append('\"');
				break;
			case NUMBER:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + "_" + i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(list.get(i).toString().trim());
				break;
			case NULL:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + "_" + i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append("null");
				break;
			case BOOLEAN:
				stringBuilder.append('\"');
				stringBuilder.append(prefix + "_" + i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(list.get(i).toBoolean());
				break;
			case ARRAY:
				flattenJson(prefix + "_" + i, list.get(i).asList(), stringBuilder);
				break;
			case OBJECT:
				flattenJson(prefix + "_" + i + "_", list.get(i), stringBuilder);
				break;
			}
			if(i < list.size() - 1) {
				stringBuilder.append(',');
			}
		}
	}
}
