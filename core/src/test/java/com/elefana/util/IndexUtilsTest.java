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
	}

	@Test
	public void testFlattenJsonString() throws IOException {
		final String json = "{\"int\": 123, \"bool\": true, \"string\": \"str\", \"array\":[100, 101, 102], \"arrayObj\":[{\"int\": 124}, {\"int\": 125}, {\"double\": 1.01234}], \"object\":{\"a\":1,\"b\":2}}";
		final StringBuilder expectedResult = new StringBuilder();
		expectedResult.append('{');
		expectedResult.append("\"int\":123,");
		expectedResult.append("\"bool\":true,");
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
}
