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
package com.elefana.es2.search;

import static io.restassured.RestAssured.given;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

import io.restassured.RestAssured;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class SearchApiTest {
	private static final int DOCUMENT_QUANTITY = 100;
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}

	@Test
	public void testFieldStats() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateDocuments(DOCUMENT_QUANTITY, index, type);
	}
	
	private void generateDocuments(int quantity, String index, String type) {
		for(int i = 0; i < quantity; i++) {
			given()
				.request()
				.body("{\"message\" : \"This is message " + i + "\",\"date\" : \"2009-11-15T14:12:12\"}")
			.when()
				.post("/" + index + "/" + type + "/")
			.then()
				.statusCode(201);
		}
	}
}
