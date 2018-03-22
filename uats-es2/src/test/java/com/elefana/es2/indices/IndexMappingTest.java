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
package com.elefana.es2.indices;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

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
public class IndexMappingTest {

	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test
	public void testMappingGeneration() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test\"}")
		.when()
			.post("/" + index + "/" + type)
		.then()
			.statusCode(201);
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
		given().when().get("/" + index + "/_mapping/" + type)
		.then()
			.statusCode(200)
			.log().all()
			.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));
	}
	
	@Test
	public void testFieldMappingGeneration() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test\"}")
		.when()
			.post("/" + index + "/" + type)
		.then()
			.statusCode(201);
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
		given().when().get("/" + index + "/_mapping/" + type + "/field/docField")
		.then()
			.statusCode(200)
			.log().all()
			.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));
	}
}
