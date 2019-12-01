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

import com.elefana.api.exception.ElefanaException;
import com.fasterxml.jackson.core.io.CharTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface IndexUtils {
	public static final SecureRandom SECURE_RANDOM = new SecureRandom();
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final String DATA_TABLE = "elefana_data";
	public static final String PARTITION_TRACKING_TABLE = "elefana_partition_tracking";

	public static final String TRIGGERS_PREFIX = "elefana_triggers_";
	public static final String HASH_INDEX_PREFIX = "elefana_hash_idx_";
	public static final String BTREE_INDEX_PREFIX = "elefana_btree_idx_";
	public static final String GIN_INDEX_PREFIX = "elefana_gin_idx_";
	public static final String BRIN_INDEX_PREFIX = "elefana_brin_idx_";
	public static final String PRIMARY_KEY_PREFIX = "elefana_pkey_";

	public static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

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

	public static final String [] ESCAPE_SEARCH = new String [] {
			"\\\"",
			"\n \"",
			"\n  \"",
			"\n   \"",
			"\n    \"",
			"\n",
			"\r",
			"\t",
			"\f",
			"\b",
			"\u0000"
	};
	public static final String [] ESCAPE_REPLACE = new String [] {
			"\\\\\\\"",
			" \"",
			" \"",
			" \"",
			" \"",
			"\\\\n",
			"\\\\r",
			"\\\\t",
			"\\\\f",
			"\\\\b",
			""
	};
	public static final String [] ESCAPE_PRE_ESCAPE = new String [] {
			"\\\\\\\"",
			"\\\\n",
			"\\\\r",
			"\\\\t",
			"\\\\f",
			"\\\\b"
	};
	
	/**
	 * If JSON string contains \" we need to escape it as \\" for PSQL to handle correctly
	 * @param json The original JSON string
	 * @return The escaped JSON string
	 */
	public static String psqlEscapeString(String json) {
		if(NoAllocStringReplace.contains(json, ESCAPE_PRE_ESCAPE)) {
			return json;
		}
		final NoAllocStringReplace str = NoAllocStringReplace.allocate(json);
		str.replaceAndEscapeUnicode(ESCAPE_SEARCH, ESCAPE_REPLACE);
		return str.dispose();

//		if(json.contains("\\\\\\")) {
//			return json;
//		}
//		json = json.replace("\u0000", "");
//		json = json.replace("\\\"", "\\\\\\\"");
//		final StringBuilder result = new StringBuilder(json);
//
//		int replaceOffset = 0;
//		for(int i = 0; i < json.length(); i++) {
//			final char c0 = json.charAt(i);
//			switch(json.charAt(i)) {
//			default:
//				break;
//			case 0x00:
//				continue;
//			case '\\':
//				switch(json.charAt(i + 1)) {
//				case '\\':
//					i++;
//					break;
//				case 'u':
//					boolean allDigits = true;
//					if(i + 5 >= json.length()) {
//						continue;
//					}
//					for(int j = i + 2; j <= i + 5 && j < json.length(); j++) {
//						if(!Character.isDigit(json.charAt(j))) {
//							allDigits = false;
//							break;
//						}
//					}
//					if(allDigits) {
//						//Unicode sequence
//						result.insert(i + replaceOffset, '\\');
//						replaceOffset ++;
//					}
//					break;
//				default:
//					continue;
//				}
//				break;
//			}
//		}
//		return result.toString();
	}
	
	public static String psqlUnescapeString(String json) {
		for(int i = 0; i < json.length(); i++) {
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
				case 0x85:
				case 'n':
					if(i > 0) {
						json = json.substring(0, i) + '\n' + json.substring(i + 3);
					} else {
						json = '\n' + json.substring(i + 3);
					}
					break;
				case 'r':
					if(i > 0) {
						json = json.substring(0, i) + '\r' + json.substring(i + 3);
					} else {
						json = '\r' + json.substring(i + 3);
					}
					break;
				case 't':
					if(i > 0) {
						json = json.substring(0, i) + '\t' + json.substring(i + 3);
					} else {
						json = '\t' + json.substring(i + 3);
					}
					break;
				case 'f':
					if(i > 0) {
						json = json.substring(0, i) + '\f' + json.substring(i + 3);
					} else {
						json = '\f' + json.substring(i + 3);
					}
					break;
				case 'b':
					if(i > 0) {
						json = json.substring(0, i) + '\b' + json.substring(i + 3);
					} else {
						json = '\b' + json.substring(i + 3);
					}
					break;
				case 'u':
					if(i > 0) {
						json = json.substring(0, i) + json.substring(i + 1);
					} else {
						json = json.substring(i + 1);
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

	public static final CumulativeAverage FLATTEN_JSON_CAPACITY = new CumulativeAverage(32);

	public static String flattenJson(String json) throws IOException {
		final NoAllocStringReplace str = NoAllocStringReplace.allocate(json);
		str.replaceAndEscapeUnicode(ESCAPE_SEARCH, ESCAPE_REPLACE);

		final StringBuilder result = POOLED_STRING_BUILDER.get();

		final JsonNode root = OBJECT_MAPPER.readTree(str.dispose());
		result.append('{');
		flattenJson("", root, result);
		result.append('}');

		FLATTEN_JSON_CAPACITY.add(result.length());
		return result.toString();
	}

	public static boolean flattenJson(String prefix, JsonNode obj, StringBuilder stringBuilder) {
		final Iterator<Map.Entry<String, JsonNode>> iterator = obj.fields();

		boolean appendedField = false;
		while(iterator.hasNext()) {
			final Map.Entry<String, JsonNode> field = iterator.next();

			if (appendedField) {
				stringBuilder.append(',');
			}

			if(field.getValue().isArray()) {
				if (field.getValue().size() > 0) {
					final String key = field.getKey();
					final PooledStringBuilder newPrefix = PooledStringBuilder.allocate();
					newPrefix.append(prefix);
					newPrefix.append(key);
					appendedField = flattenJson(newPrefix.toString(), field.getValue().elements(), stringBuilder);
					newPrefix.release();
				} else {
					stringBuilder.append('\"');
					stringBuilder.append(prefix);
					stringBuilder.append(field.getKey());
					stringBuilder.append('\"');
					stringBuilder.append(':');
					stringBuilder.append("null");
					appendedField = true;
				}
			} else if(field.getValue().isObject()) {
				if(field.getValue().size() > 0) {
					final String key = field.getKey();
					final PooledStringBuilder newPrefix = PooledStringBuilder.allocate();
					newPrefix.append(prefix);
					newPrefix.append(key);
					newPrefix.append('_');
					appendedField = flattenJson(newPrefix.toString(), field.getValue(), stringBuilder);
					newPrefix.release();
				} else {
					stringBuilder.append('\"');
					stringBuilder.append(prefix);
					stringBuilder.append(field.getKey());
					stringBuilder.append('\"');
					stringBuilder.append(':');
					stringBuilder.append("null");
					appendedField = true;
				}
			} else if(field.getValue().isNull()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append(field.getKey());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append("null");
				appendedField = true;
			} else if(field.getValue().isNumber()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append(field.getKey());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(field.getValue().numberValue());
				appendedField = true;
			} else if(field.getValue().isBoolean()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append(field.getKey());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(field.getValue().booleanValue());
				appendedField = true;
			} else if(field.getValue().isTextual()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append(field.getKey());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append('\"');
				CharTypes.appendQuoted(stringBuilder, field.getValue().textValue());
				stringBuilder.append('\"');
				appendedField = true;
			} else {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append(field.getKey());
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(field.getValue().asText());
				appendedField = true;
			}
		}
		return appendedField;
	}

	public static boolean flattenJson(String prefix, Iterator<JsonNode> list, StringBuilder stringBuilder) {
		boolean appendedField = false;
		int i = 0;
		while(list.hasNext()) {
			final JsonNode nextNode = list.next();

			if(appendedField) {
				stringBuilder.append(',');
			}

			if(nextNode.isArray()) {
				if (nextNode.size() > 0) {
					final PooledStringBuilder newPrefix = PooledStringBuilder.allocate();
					newPrefix.append(prefix);
					newPrefix.append('_');
					newPrefix.append(i);
					appendedField = flattenJson(newPrefix.toString(), nextNode.elements(), stringBuilder);
					newPrefix.release();
				} else {
					stringBuilder.append('\"');
					stringBuilder.append(prefix);
					stringBuilder.append('_');
					stringBuilder.append(i);
					stringBuilder.append('\"');
					stringBuilder.append(':');
					stringBuilder.append("null");
					appendedField = true;
				}
			} else if(nextNode.isObject()) {
				if(nextNode.size() > 0) {
					final PooledStringBuilder newPrefix = PooledStringBuilder.allocate();
					newPrefix.append(prefix);
					newPrefix.append('_');
					newPrefix.append(i);
					newPrefix.append('_');
					appendedField = flattenJson(newPrefix.toString(), nextNode, stringBuilder);
					newPrefix.release();
				} else {
					stringBuilder.append('\"');
					stringBuilder.append(prefix);
					stringBuilder.append('_');
					stringBuilder.append(i);
					stringBuilder.append('\"');
					stringBuilder.append(':');
					stringBuilder.append("null");
					appendedField = true;
				}
			} else if(nextNode.isNull()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append('_');
				stringBuilder.append(i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append("null");
				appendedField = true;
			} else if(nextNode.isNumber()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append('_');
				stringBuilder.append(i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(nextNode.numberValue());
				appendedField = true;
			} else if(nextNode.isBoolean()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append('_');
				stringBuilder.append(i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(nextNode.booleanValue());
				appendedField = true;
			} else if(nextNode.isTextual()) {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append('_');
				stringBuilder.append(i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append('\"');
				CharTypes.appendQuoted(stringBuilder, nextNode.textValue());
				stringBuilder.append('\"');
				appendedField = true;
			} else {
				stringBuilder.append('\"');
				stringBuilder.append(prefix);
				stringBuilder.append('_');
				stringBuilder.append(i);
				stringBuilder.append('\"');
				stringBuilder.append(':');
				stringBuilder.append(nextNode.toString());
				appendedField = true;
			}
			i++;
		}
		return appendedField;
	}

	public static String createTempFilePath(String prefix, String suffix, File directory) {
		if(prefix == null) {
			prefix = "";
		}
		if(prefix.contains("/")) {
			prefix = prefix.substring(prefix.lastIndexOf('/') + 1);
		}
		if(suffix == null) {
			suffix = ".tmp";
		}

		File result = null;
		do {
			long n = SECURE_RANDOM.nextLong();
			if (n == Long.MIN_VALUE) {
				n = 0;
			} else {
				n = Math.abs(n);
			}

			final String filename = prefix + Long.toString(n) + suffix;
			result = new File(directory, filename);
		} while (result.exists());
		return result.getAbsolutePath();
	}

	public static String bytesToHex(byte[] bytes) {
		char[] result = new char[bytes.length * 2];
		for ( int j = 0; j < bytes.length; j++ ) {
			int v = bytes[j] & 0xFF;
			result[j * 2] = HEX_CHARS[v >>> 4];
			result[j * 2 + 1] = HEX_CHARS[v & 0x0F];
		}
		return new String(result);
	}

	public static String bytesToHex(ByteBuf bytes) {
		final int totalBytes = (bytes.writerIndex() - bytes.readerIndex());
		char[] result = new char[totalBytes * 2];
		for ( int j = 0; j < totalBytes; j++ ) {
			int v = bytes.readByte() & 0xFF;
			result[j * 2] = HEX_CHARS[v >>> 4];
			result[j * 2 + 1] = HEX_CHARS[v & 0x0F];
		}
		return new String(result);
	}

	public static ThreadLocal<StringBuilder> POOLED_STRING_BUILDER = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder(1024);
		}

		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0); // clear/reset the buffer
			return b;
		}
	};
}
