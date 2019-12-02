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

import java.io.IOException;
import java.util.Scanner;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import org.junit.Assert;
import org.junit.Test;


public class IndexUtilsTest {

	@Test
	public void testPsqlEscapeJsonString() throws IOException {
		final String inputWithoutJson = "\"This is a message";
		final String expectedWithoutJson = "\"This is a message";
		
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlEscapeString(inputWithoutJson));
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlEscapeString(expectedWithoutJson));

		final String inputJson = new Scanner(IndexUtilsTest.class.getResource("/escapedSample.json").openStream()).nextLine();
		final String expectedJson = inputJson.replace("\\", "\\\\\\");
		
		Assert.assertEquals(expectedJson, IndexUtils.psqlEscapeString(inputJson));
		Assert.assertEquals(expectedJson, IndexUtils.psqlEscapeString(expectedJson));
		
		final String inputJson2 = "\\\"Nested json\\\"";
		final String expectedJson2 = "\\\\\\\"Nested json\\\\\\\"";
		
		Assert.assertEquals(expectedJson2, IndexUtils.psqlEscapeString(inputJson2));
		Assert.assertEquals(expectedJson2, IndexUtils.psqlEscapeString(expectedJson2));
		
		final String inputJson3 = "\\\"Nested\\json\\\"";
		final String expectedJson3 = "\\\\\\\"Nested\\json\\\\\\\"";
		
		Assert.assertEquals(expectedJson3, IndexUtils.psqlEscapeString(inputJson3));
		Assert.assertEquals(expectedJson3, IndexUtils.psqlEscapeString(expectedJson3));
		
		final String inputJson4 = "\\\"Nested\"json\\\"";
		final String expectedJson4 = "\\\\\\\"Nested\"json\\\\\\\"";
		
		Assert.assertEquals(expectedJson4, IndexUtils.psqlEscapeString(inputJson4));
		Assert.assertEquals(expectedJson4, IndexUtils.psqlEscapeString(expectedJson4));

		final String inputJson5 = "\\\"Nested\njson\\\"";
		final String expectedJson5 = "\\\\\\\"Nested\\\\njson\\\\\\\"";

		Assert.assertEquals(expectedJson5, IndexUtils.psqlEscapeString(inputJson5));
		Assert.assertEquals(expectedJson5, IndexUtils.psqlEscapeString(expectedJson5));

		final String inputJson6 = "\n";
		final String expectedJson6 = "\\\\n";

		Assert.assertEquals(expectedJson6, IndexUtils.psqlEscapeString(inputJson6));
		Assert.assertEquals(expectedJson6, IndexUtils.psqlEscapeString(expectedJson6));

		final String inputJson7 = "Nested\rjson";
		final String expectedJson7 = "Nested\\\\rjson";

		Assert.assertEquals(expectedJson7, IndexUtils.psqlEscapeString(inputJson7));
		Assert.assertEquals(expectedJson7, IndexUtils.psqlEscapeString(expectedJson7));

		final String inputJson8 = "Nested\r\njson";
		final String expectedJson8 = "Nested\\\\r\\\\njson";

		Assert.assertEquals(expectedJson8, IndexUtils.psqlEscapeString(inputJson8));
		Assert.assertEquals(expectedJson8, IndexUtils.psqlEscapeString(expectedJson8));

		final String inputJson9 = "Nested\tjson";
		final String expectedJson9 = "Nested\\\\tjson";

		Assert.assertEquals(expectedJson9, IndexUtils.psqlEscapeString(inputJson9));
		Assert.assertEquals(expectedJson9, IndexUtils.psqlEscapeString(expectedJson9));

		final String inputJson10 = "Nested\fjson";
		final String expectedJson10 = "Nested\\\\fjson";

		Assert.assertEquals(expectedJson10, IndexUtils.psqlEscapeString(inputJson10));
		Assert.assertEquals(expectedJson10, IndexUtils.psqlEscapeString(expectedJson10));

		final String inputJson11 = "Nested\bjson";
		final String expectedJson11 = "Nested\\\\bjson";

		Assert.assertEquals(expectedJson11, IndexUtils.psqlEscapeString(inputJson11));
		Assert.assertEquals(expectedJson11, IndexUtils.psqlEscapeString(expectedJson11));

//		final String inputJson12 = ((char) 0) + "\u0000{}";
//		final String expectedJson12 = "{}";
//
//		Assert.assertEquals(expectedJson12, IndexUtils.psqlEscapeString(inputJson12));
//		Assert.assertEquals(expectedJson12, IndexUtils.psqlEscapeString(expectedJson12));

		final String inputJson13 = "{\u0000" + ((char) 0) + "}";
		final String expectedJson13 = "{}";

		Assert.assertEquals(expectedJson13, IndexUtils.psqlEscapeString(inputJson13));
		Assert.assertEquals(expectedJson13, IndexUtils.psqlEscapeString(expectedJson13));

		final String inputJson14 = "Nested\\u0001json";
		final String expectedJson14 = "Nested\\\\u0001json";

		Assert.assertEquals(expectedJson14, IndexUtils.psqlEscapeString(inputJson14));
		Assert.assertEquals(expectedJson14, IndexUtils.psqlEscapeString(expectedJson14));
	}
	
	@Test
	public void testPsqlUnescapeJsonString() throws IOException {
		final String inputWithoutJson = "\"This is a message";
		final String expectedWithoutJson  = "\"This is a message";
		
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlUnescapeString(inputWithoutJson));
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlUnescapeString(expectedWithoutJson));
		
		final String expectedJson = new Scanner(IndexUtilsTest.class.getResource("/escapedSample.json").openStream()).nextLine();
		final String inputJson = expectedJson.replace("\\", "\\\\\\");
		
		Assert.assertEquals(expectedJson, IndexUtils.psqlUnescapeString(inputJson));
		Assert.assertEquals(expectedJson, IndexUtils.psqlUnescapeString(expectedJson));
		
		final String inputJson2 = "\\\\\\\"Nested json\\\\\\\"";
		final String expectedJson2 = "\\\"Nested json\\\"";
		
		Assert.assertEquals(expectedJson2, IndexUtils.psqlUnescapeString(inputJson2));
		Assert.assertEquals(expectedJson2, IndexUtils.psqlUnescapeString(expectedJson2));
		
		final String inputJson3 = "\\\\\\\"Nested\\json\\\\\\\"";
		final String expectedJson3 = "\\\"Nested\\json\\\"";
		
		Assert.assertEquals(expectedJson3, IndexUtils.psqlUnescapeString(inputJson3));
		Assert.assertEquals(expectedJson3, IndexUtils.psqlUnescapeString(expectedJson3));
		
		final String inputJson4 = "\\\\\\\"Nested\"json\\\\\\\"";
		final String expectedJson4 = "\\\"Nested\"json\\\"";
		
		Assert.assertEquals(expectedJson4, IndexUtils.psqlUnescapeString(inputJson4));
		Assert.assertEquals(expectedJson4, IndexUtils.psqlUnescapeString(expectedJson4));

		final String inputJson5 = "\\\\\\\"Nested\\\\njson\\\\\\\"";
		final String expectedJson5 = "\\\"Nested\njson\\\"";

		Assert.assertEquals(expectedJson5, IndexUtils.psqlUnescapeString(inputJson5));
		Assert.assertEquals(expectedJson5, IndexUtils.psqlUnescapeString(expectedJson5));

		final String inputJson6 = "\\\\n";
		final String expectedJson6 = "\n";

		Assert.assertEquals(expectedJson6, IndexUtils.psqlUnescapeString(inputJson6));
		Assert.assertEquals(expectedJson6, IndexUtils.psqlUnescapeString(expectedJson6));

		final String inputJson7 = "Nested\\\\rjson";
		final String expectedJson7 = "Nested\rjson";

		Assert.assertEquals(expectedJson7, IndexUtils.psqlUnescapeString(inputJson7));
		Assert.assertEquals(expectedJson7, IndexUtils.psqlUnescapeString(expectedJson7));

		final String inputJson8 = "Nested\\\\r\\\\njson";
		final String expectedJson8 = "Nested\r\njson";

		Assert.assertEquals(expectedJson8, IndexUtils.psqlUnescapeString(inputJson8));
		Assert.assertEquals(expectedJson8, IndexUtils.psqlUnescapeString(expectedJson8));

		final String inputJson9 = "Nested\\\\tjson";
		final String expectedJson9 = "Nested\tjson";

		Assert.assertEquals(expectedJson9, IndexUtils.psqlUnescapeString(inputJson9));
		Assert.assertEquals(expectedJson9, IndexUtils.psqlUnescapeString(expectedJson9));

		final String inputJson10 = "Nested\\\\fjson";
		final String expectedJson10 = "Nested\fjson";

		Assert.assertEquals(expectedJson10, IndexUtils.psqlUnescapeString(inputJson10));
		Assert.assertEquals(expectedJson10, IndexUtils.psqlUnescapeString(expectedJson10));

		final String inputJson11 = "Nested\\\\bjson";
		final String expectedJson11 = "Nested\bjson";

		Assert.assertEquals(expectedJson11, IndexUtils.psqlUnescapeString(inputJson11));
		Assert.assertEquals(expectedJson11, IndexUtils.psqlUnescapeString(expectedJson11));

		final String inputJson14 = "Nested\\\\u0000json";
		final String expectedJson14 = "Nested\\u0000json";

		Assert.assertEquals(expectedJson14, IndexUtils.psqlUnescapeString(inputJson14));
		Assert.assertEquals(expectedJson14, IndexUtils.psqlUnescapeString(expectedJson14));
	}

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
		final Any any = JsonIterator.deserialize(result);
		Assert.assertEquals(ValueType.STRING, any.get("str").valueType());
	}

	@Test
	public void testFlattenJsonStringWithLineBreakBetweenContent() throws IOException {
		final String json = "{\"str\":\"This a value with a line break\",\n \"int\": 0}";
		final String result = IndexUtils.flattenJson(json);
		final Any any = JsonIterator.deserialize(result);
		Assert.assertEquals(ValueType.STRING, any.get("str").valueType());
	}

	@Test
	public void testFlattenJsonStringWithLineBreakInAndBetweenContent() throws IOException {
		final String json = "{\"str\":\"This a value with\n a line break\",\n \"int\": 0}";
		final String result = IndexUtils.flattenJson(json);
		final Any any = JsonIterator.deserialize(result);
		Assert.assertEquals(ValueType.STRING, any.get("str").valueType());
	}

	@Test
	public void testFlattenJsonStringWithTab() throws IOException {
		final String json = "{\"str\":\"This a value with a \t tab\"}";
		final String result = IndexUtils.flattenJson(json);
		final Any any = JsonIterator.deserialize(result);
		Assert.assertEquals(ValueType.STRING, any.get("str").valueType());
	}
}
