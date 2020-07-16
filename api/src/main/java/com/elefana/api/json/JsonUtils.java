/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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
package com.elefana.api.json;

import com.elefana.api.document.BulkResponse;
import com.elefana.api.indices.GetFieldMappingsResponse;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.GetIndexTemplateResponse;
import com.elefana.api.node.NodeStats;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.api.util.ThreadLocalCharArray;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public class JsonUtils {
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper() {
		{
			configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

			final SimpleModule simpleModule = new SimpleModule();
			simpleModule.addDeserializer(BulkResponse.class, new BulkResponseDecoder());
			simpleModule.addDeserializer(GetFieldMappingsResponse.class, new GetFieldMappingsResponseDecoder());
			simpleModule.addDeserializer(GetIndexTemplateResponse.class, new GetIndexTemplateResponseDecoder());
			simpleModule.addDeserializer(GetIndexTemplateForIndexResponse.class, new GetIndexTemplateForIndexResponseDecoder());
			simpleModule.addDeserializer(NodeStats.class, new NodeStatsDecoder());
			registerModule(simpleModule);
		}
	};
	public static final JsonFactory JSON_FACTORY = new JsonFactory() {
		{
			setCodec(OBJECT_MAPPER);
			enable(JsonFactory.Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING);

			enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
			enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
		}
	};
	private static final ThreadLocalCharArray CHAR_ARRAY = new ThreadLocalCharArray();

	public static JsonNode extractJsonNode(String json, String... path) {
		JsonNode result = null;
		try {
			result = OBJECT_MAPPER.readTree(json);
			if(path != null) {
				for(int i = 0; i < path.length; i++) {
					result = result.get(path[i]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static JsonNode extractJsonNode(char [] json, int jsonLength, String... path) {
		JsonNode result = null;
		JsonParser jsonParser = null;
		try {
			jsonParser = JSON_FACTORY.createParser(json, 0, jsonLength);
			result = jsonParser.readValueAs(JsonNode.class);
			if(path != null) {
				for(int i = 0; i < path.length; i++) {
					result = result.get(path[i]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				jsonParser.close();
			} catch (IOException e) {}
		}
		return result;
	}

	public static JsonNode extractJsonNode(PooledStringBuilder str, String... path) {
		char [] chars = CHAR_ARRAY.get();
		if(chars.length < str.length()) {
			chars = new char[str.length()];
		}
		str.getChars(chars);
		final JsonNode result = extractJsonNode(chars, str.length(), path);
		CHAR_ARRAY.set(chars);
		return result;
	}

	public static <T> String toJsonString(T object) {
		try {
			return OBJECT_MAPPER.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static <T> T fromJsonString(char [] json, Class<T> clazz) {
		try {
			return fromJsonString(JSON_FACTORY.createParser(json), clazz);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static <T> T fromJsonString(byte [] json, Class<T> clazz) {
		try {
			return fromJsonString(JSON_FACTORY.createParser(json), clazz);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static <T> T fromJsonString(String json, Class<T> clazz) {
		try {
			return fromJsonString(JSON_FACTORY.createParser(json), clazz);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static <T> T fromJsonString(JsonParser jsonParser, Class<T> clazz) {
		try {
			final T result = OBJECT_MAPPER.readValue(jsonParser, clazz);
			jsonParser.close();
			return result;
		} catch (IOException e) {
			e.printStackTrace();
			try {
				jsonParser.close();
			} catch (IOException ex) {}
		}
		return null;
	}
}
