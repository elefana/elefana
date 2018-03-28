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

import org.junit.Assert;
import org.junit.Test;


public class IndexUtilsTest {

	@Test
	public void testPsqlEscapeJsonString() {
		final String inputWithoutJson = "\"This is a message";
		final String expectedWithoutJson = "\"This is a message";
		
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlEscapeString(inputWithoutJson));
		Assert.assertEquals(expectedWithoutJson, IndexUtils.psqlEscapeString(expectedWithoutJson));
		
		final String inputJson = "{\\\"Nested json\\\"}";
		final String expectedJson = "{\\\\\\\"Nested json\\\\\\\"}";
		
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
}