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
package com.elefana.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Scanner;

public class EscapeUtilsTest {
	@Test
	public void testPsqlEscapeJsonString() throws IOException {
		final String inputWithoutJson = "\"This is a message";
		final String expectedWithoutJson = "\"This is a message";

		Assert.assertEquals(expectedWithoutJson, EscapeUtils.psqlEscapeString(inputWithoutJson));
		Assert.assertEquals(expectedWithoutJson, EscapeUtils.psqlEscapeString(expectedWithoutJson));

		final String inputJson = new Scanner(IndexUtilsTest.class.getResource("/escapedSample.json").openStream()).nextLine();
		final String expectedJson = inputJson.replace("\\", "\\\\\\");

		Assert.assertEquals(expectedJson, EscapeUtils.psqlEscapeString(inputJson));
		Assert.assertEquals(expectedJson, EscapeUtils.psqlEscapeString(expectedJson));

		final String inputJson2 = "\\\"Nested json\\\"";
		final String expectedJson2 = "\\\\\\\"Nested json\\\\\\\"";

		Assert.assertEquals(expectedJson2, EscapeUtils.psqlEscapeString(inputJson2));
		Assert.assertEquals(expectedJson2, EscapeUtils.psqlEscapeString(expectedJson2));

		final String inputJson3 = "\\\"Nested\\json\\\"";
		final String expectedJson3 = "\\\\\\\"Nested\\json\\\\\\\"";

		Assert.assertEquals(expectedJson3, EscapeUtils.psqlEscapeString(inputJson3));
		Assert.assertEquals(expectedJson3, EscapeUtils.psqlEscapeString(expectedJson3));

		final String inputJson4 = "\\\"Nested\"json\\\"";
		final String expectedJson4 = "\\\\\\\"Nested\"json\\\\\\\"";

		Assert.assertEquals(expectedJson4, EscapeUtils.psqlEscapeString(inputJson4));
		Assert.assertEquals(expectedJson4, EscapeUtils.psqlEscapeString(expectedJson4));

		final String inputJson5 = "\\\"Nested\njson\\\"";
		final String expectedJson5 = "\\\\\\\"Nested\\\\njson\\\\\\\"";

		Assert.assertEquals(expectedJson5, EscapeUtils.psqlEscapeString(inputJson5));
		Assert.assertEquals(expectedJson5, EscapeUtils.psqlEscapeString(expectedJson5));

		final String inputJson6 = "\n";
		final String expectedJson6 = "\\\\n";

		Assert.assertEquals(expectedJson6, EscapeUtils.psqlEscapeString(inputJson6));
		Assert.assertEquals(expectedJson6, EscapeUtils.psqlEscapeString(expectedJson6));

		final String inputJson7 = "Nested\rjson";
		final String expectedJson7 = "Nested\\\\rjson";

		Assert.assertEquals(expectedJson7, EscapeUtils.psqlEscapeString(inputJson7));
		Assert.assertEquals(expectedJson7, EscapeUtils.psqlEscapeString(expectedJson7));

		final String inputJson8 = "Nested\r\njson";
		final String expectedJson8 = "Nested\\\\r\\\\njson";

		Assert.assertEquals(expectedJson8, EscapeUtils.psqlEscapeString(inputJson8));
		Assert.assertEquals(expectedJson8, EscapeUtils.psqlEscapeString(expectedJson8));

		final String inputJson9 = "Nested\tjson";
		final String expectedJson9 = "Nested\\\\tjson";

		Assert.assertEquals(expectedJson9, EscapeUtils.psqlEscapeString(inputJson9));
		Assert.assertEquals(expectedJson9, EscapeUtils.psqlEscapeString(expectedJson9));

		final String inputJson10 = "Nested\fjson";
		final String expectedJson10 = "Nested\\\\fjson";

		Assert.assertEquals(expectedJson10, EscapeUtils.psqlEscapeString(inputJson10));
		Assert.assertEquals(expectedJson10, EscapeUtils.psqlEscapeString(expectedJson10));

		final String inputJson11 = "Nested\bjson";
		final String expectedJson11 = "Nested\\\\bjson";

		Assert.assertEquals(expectedJson11, EscapeUtils.psqlEscapeString(inputJson11));
		Assert.assertEquals(expectedJson11, EscapeUtils.psqlEscapeString(expectedJson11));

//		final String inputJson12 = ((char) 0) + "\u0000{}";
//		final String expectedJson12 = "{}";
//
//		Assert.assertEquals(expectedJson12, EscapeUtils.psqlEscapeString(inputJson12));
//		Assert.assertEquals(expectedJson12, EscapeUtils.psqlEscapeString(expectedJson12));

		final String inputJson13 = "{\u0000" + ((char) 0) + "}";
		final String expectedJson13 = "{}";

		Assert.assertEquals(expectedJson13, EscapeUtils.psqlEscapeString(inputJson13));
		Assert.assertEquals(expectedJson13, EscapeUtils.psqlEscapeString(expectedJson13));

		final String inputJson14 = "Nested\\u0001json";
		final String expectedJson14 = "Nested\\\\u0001json";

		Assert.assertEquals(expectedJson14, EscapeUtils.psqlEscapeString(inputJson14));
		Assert.assertEquals(expectedJson14, EscapeUtils.psqlEscapeString(expectedJson14));
	}

	@Test
	public void testPsqlUnescapeJsonString() throws IOException {
		final String inputWithoutJson = "\"This is a message";
		final String expectedWithoutJson  = "\"This is a message";

		Assert.assertEquals(expectedWithoutJson, EscapeUtils.psqlUnescapeString(inputWithoutJson));
		Assert.assertEquals(expectedWithoutJson, EscapeUtils.psqlUnescapeString(expectedWithoutJson));

		final String expectedJson = new Scanner(IndexUtilsTest.class.getResource("/escapedSample.json").openStream()).nextLine();
		final String inputJson = expectedJson.replace("\\", "\\\\\\");

		Assert.assertEquals(expectedJson, EscapeUtils.psqlUnescapeString(inputJson));
		Assert.assertEquals(expectedJson, EscapeUtils.psqlUnescapeString(expectedJson));

		final String inputJson2 = "\\\\\\\"Nested json\\\\\\\"";
		final String expectedJson2 = "\\\"Nested json\\\"";

		Assert.assertEquals(expectedJson2, EscapeUtils.psqlUnescapeString(inputJson2));
		Assert.assertEquals(expectedJson2, EscapeUtils.psqlUnescapeString(expectedJson2));

		final String inputJson3 = "\\\\\\\"Nested\\json\\\\\\\"";
		final String expectedJson3 = "\\\"Nested\\json\\\"";

		Assert.assertEquals(expectedJson3, EscapeUtils.psqlUnescapeString(inputJson3));
		Assert.assertEquals(expectedJson3, EscapeUtils.psqlUnescapeString(expectedJson3));

		final String inputJson4 = "\\\\\\\"Nested\"json\\\\\\\"";
		final String expectedJson4 = "\\\"Nested\"json\\\"";

		Assert.assertEquals(expectedJson4, EscapeUtils.psqlUnescapeString(inputJson4));
		Assert.assertEquals(expectedJson4, EscapeUtils.psqlUnescapeString(expectedJson4));

		final String inputJson5 = "\\\\\\\"Nested\\\\njson\\\\\\\"";
		final String expectedJson5 = "\\\"Nested\njson\\\"";

		Assert.assertEquals(expectedJson5, EscapeUtils.psqlUnescapeString(inputJson5));
		Assert.assertEquals(expectedJson5, EscapeUtils.psqlUnescapeString(expectedJson5));

		final String inputJson6 = "\\\\n";
		final String expectedJson6 = "\n";

		Assert.assertEquals(expectedJson6, EscapeUtils.psqlUnescapeString(inputJson6));
		Assert.assertEquals(expectedJson6, EscapeUtils.psqlUnescapeString(expectedJson6));

		final String inputJson7 = "Nested\\\\rjson";
		final String expectedJson7 = "Nested\rjson";

		Assert.assertEquals(expectedJson7, EscapeUtils.psqlUnescapeString(inputJson7));
		Assert.assertEquals(expectedJson7, EscapeUtils.psqlUnescapeString(expectedJson7));

		final String inputJson8 = "Nested\\\\r\\\\njson";
		final String expectedJson8 = "Nested\r\njson";

		Assert.assertEquals(expectedJson8, EscapeUtils.psqlUnescapeString(inputJson8));
		Assert.assertEquals(expectedJson8, EscapeUtils.psqlUnescapeString(expectedJson8));

		final String inputJson9 = "Nested\\\\tjson";
		final String expectedJson9 = "Nested\tjson";

		Assert.assertEquals(expectedJson9, EscapeUtils.psqlUnescapeString(inputJson9));
		Assert.assertEquals(expectedJson9, EscapeUtils.psqlUnescapeString(expectedJson9));

		final String inputJson10 = "Nested\\\\fjson";
		final String expectedJson10 = "Nested\fjson";

		Assert.assertEquals(expectedJson10, EscapeUtils.psqlUnescapeString(inputJson10));
		Assert.assertEquals(expectedJson10, EscapeUtils.psqlUnescapeString(expectedJson10));

		final String inputJson11 = "Nested\\\\bjson";
		final String expectedJson11 = "Nested\bjson";

		Assert.assertEquals(expectedJson11, EscapeUtils.psqlUnescapeString(inputJson11));
		Assert.assertEquals(expectedJson11, EscapeUtils.psqlUnescapeString(expectedJson11));

		final String inputJson14 = "Nested\\\\u0000json";
		final String expectedJson14 = "Nested\\u0000json";

		Assert.assertEquals(expectedJson14, EscapeUtils.psqlUnescapeString(inputJson14));
		Assert.assertEquals(expectedJson14, EscapeUtils.psqlUnescapeString(expectedJson14));
	}

}
