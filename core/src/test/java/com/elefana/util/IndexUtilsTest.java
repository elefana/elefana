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

import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class IndexUtilsTest {

	@Test
	public void testFlattenJsonString() throws IOException {
		final String json = "{\"nestedJson\": \"{\\\"nestedKey\\\":\\\"nestedValue\\\"}\", \"int\": 123, \"bool\": true, \"long\": 1234567890123, \"string\": \"str\", \"array\":[100, 101, 102], \"arrayObj\":[{\"int\": 124}, {\"int\": 125}, {\"double\": 1.01234}], \"object\":{\"a\":1,\"b\":2}}";
		final StringBuilder expectedResult = new StringBuilder();
		expectedResult.append('{');
		expectedResult.append("\"nestedJson\":\"{\\\\\\\"nestedKey\\\\\\\":\\\\\\\"nestedValue\\\\\\\"}\",");
		expectedResult.append("\"int\":123,");
		expectedResult.append("\"bool\":true,");
		expectedResult.append("\"long\":1234567890123,");
		expectedResult.append("\"string\":\"str\",");
		expectedResult.append("\"array_0\":100,");
		expectedResult.append("\"array_1\":101,");
		expectedResult.append("\"array_2\":102,");
		expectedResult.append("\"arrayObj_0_int\":124,");
		expectedResult.append("\"arrayObj_1_int\":125,");
		expectedResult.append("\"arrayObj_2_double\":1.01234,");
		expectedResult.append("\"object_a\":1,");
		expectedResult.append("\"object_b\":2");
		expectedResult.append('}');

		Assert.assertEquals(expectedResult.toString(), IndexUtils.flattenJson(json));
	}

	@Test
	public void testFlattenJsonStringWithLineBreakInContent() throws IOException {
		final String json = "{\"str\":\"This a value with a \n line break\"}";
		final String result = IndexUtils.flattenJson(json);
		final JsonNode any = JsonUtils.extractJsonNode(result);
		Assert.assertEquals(JsonToken.VALUE_STRING, any.get("str").asToken());
	}

	@Test
	public void testFlattenJsonStringWithLineBreakBetweenContent() throws IOException {
		final String json = "{\"str\":\"This a value with a line break\",\n \"int\": 0}";
		final String result = IndexUtils.flattenJson(json);
		final JsonNode any =  JsonUtils.extractJsonNode(result);
		Assert.assertEquals(JsonToken.VALUE_STRING, any.get("str").asToken());
	}

	@Test
	public void testFlattenJsonStringWithLineBreakInAndBetweenContent() throws IOException {
		final String json = "{\"str\":\"This a value with\n a line break\",\n \"int\": 0}";
		final String result = IndexUtils.flattenJson(json);
		final JsonNode any =  JsonUtils.extractJsonNode(result);
		Assert.assertEquals(JsonToken.VALUE_STRING, any.get("str").asToken());
	}

	@Test
	public void testFlattenJsonStringWithTab() throws IOException {
		final String json = "{\"str\":\"This a value with a \t tab\"}";
		final String result = IndexUtils.flattenJson(json);
		final JsonNode any =  JsonUtils.extractJsonNode(result);
		Assert.assertEquals(JsonToken.VALUE_STRING, any.get("str").asToken());
	}
}
