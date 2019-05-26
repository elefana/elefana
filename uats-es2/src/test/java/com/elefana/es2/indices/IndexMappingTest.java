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

import java.util.List;
import java.util.UUID;

import com.elefana.TestUtils;
import io.restassured.path.json.exception.JsonPathException;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.junit.Assert;
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
	private static final long TEST_TIMEOUT = 120000L;

	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}

	@Test
	public void testFieldNames() {
		final String index = UUID.randomUUID().toString();
		final String type1 = "test1";
		final String type2 = "test2";

		given()
				.request()
				.body("{\"docField\" : \"This is a test\", \"numField\": 123, " +
						"\"objectField\": {\"nestedNumField\": 124}, " +
						"\"listField\": [{\"nestedNumField2\": 125}], " +
						"\"emptyField\": \"\"}")
				.when()
				.post("/" + index + "/" + type1)
				.then()
				.statusCode(201);

		given()
				.request()
				.body("{\"docField\" : \"This is a test 2\", \"numField\": 124}")
				.when()
				.post("/" + index + "/" + type2)
				.then()
				.statusCode(201);

		final long startTime = System.currentTimeMillis();
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			ValidatableResponse response = given().when().get("/" + index + "/_field_names")
					.then()
					.statusCode(200);

			try {
				List<String> result = response.extract().body().jsonPath().getList("field_names");
				if(result.size() < 5) {
					try {
						Thread.sleep(500L);
					} catch (Exception e) {}
					continue;
				}
				Assert.assertEquals(5, result.size());
				Assert.assertEquals(true, result.contains("docField"));
				Assert.assertEquals(true, result.contains("emptyField"));
				Assert.assertEquals(true, result.contains("numField"));
				Assert.assertEquals(true, result.contains("objectField.nestedNumField"));
				Assert.assertEquals(true, result.contains("listField[0].nestedNumField2"));

				response = given().when().get("/" + index + "/_field_names/" + type1)
						.then()
						.statusCode(200);
				result = response.extract().body().jsonPath().getList("field_names");
				Assert.assertEquals(5, result.size());
				Assert.assertEquals(true, result.contains("docField"));
				Assert.assertEquals(true, result.contains("emptyField"));
				Assert.assertEquals(true, result.contains("numField"));
				Assert.assertEquals(true, result.contains("objectField.nestedNumField"));
				Assert.assertEquals(true, result.contains("listField[0].nestedNumField2"));

				response = given().when().get("/" + index + "/_field_names/" + type2)
						.then()
						.statusCode(200);
				result = response.extract().body().jsonPath().getList("field_names");
				Assert.assertEquals(2, result.size());
				Assert.assertEquals(true, result.contains("docField"));
				Assert.assertEquals(true, result.contains("numField"));
				Assert.assertEquals(false, result.contains("emptyField"));
				Assert.assertEquals(false, result.contains("objectField.nestedNumField"));
				Assert.assertEquals(false, result.contains("listField[0].nestedNumField2"));
				return;
			} catch (JsonPathException e) {
			}

			try {
				Thread.sleep(500L);
			} catch (Exception e) {}
		}
		Assert.fail("Field names not generated within timeout");
	}
	
	@Test
	public void testFieldMappingGeneration() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test\", \"numField\": 123, \"emptyField\": \"\"}")
		.when()
			.post("/" + index + "/" + type)
		.then()
			.statusCode(201);
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test 2\", \"numField\": 124}")
		.when()
			.post("/" + index + "/" + type)
		.then()
			.statusCode(201);

		final long startTime = System.currentTimeMillis();

		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given().when().get("/" + index + "/_mapping/" + type)
					.then()
					.statusCode(200);

			try {
				final String testValue = response.extract().body().jsonPath().getString(
						index + ".mappings." + type + ".docField.mapping.docField.type");
				if(testValue != null) {
					given().when().get("/" + index + "/_mapping/" + type)
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"))
							.body(index + ".mappings." + type + ".emptyField.mapping.emptyField.type", equalTo("string"))
							.body(index + ".mappings." + type + ".numField.mapping.numField.type", equalTo("long"))
							.body(index + ".mappings." + type + "._source.full_name", equalTo("_source"));

					given().when().get("/" + index + "/_mapping/" + type + "/field/*")
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"))
							.body(index + ".mappings." + type + ".emptyField.mapping.emptyField.type", equalTo("string"))
							.body(index + ".mappings." + type + ".numField.mapping.numField.type", equalTo("long"))
							.body(index + ".mappings." + type + "._source.full_name", equalTo("_source"));

					given().when().get("/" + index + "/_mapping/" + type + "/field/numField")
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + ".numField.mapping.numField.type", equalTo("long"));

					given().when().get("/" + index + "/_mapping/" + type + "/field/docField")
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));

					given().when().get("/" + index + "/_mapping/field/docField")
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));

					given().when().get("/" + index + "/_mapping/*/field/_source")
							.then()
							.statusCode(200)
							.body(index + ".mappings." + type + "._source.full_name", equalTo("_source"));
					return;
				}
			} catch (JsonPathException e) {}

			try {
				Thread.sleep(500);
			} catch (Exception e) {}
		}
		Assert.fail("Failed to generate mappins within timeout");
	}
	
	@Test
	public void testIndexRefresh() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test\", \"numField\": 123, \"emptyField\": \"\"}")
		.when()
			.post("/" + index + "/" + type)
		.then()
			.statusCode(201);
		
		given()
			.request()
		.when()
			.post("/" + index + "/_refresh")
		.then()
			.statusCode(200).body("_shards.successful", equalTo(1));
	}
	
	@Test
	public void testFieldStatsGeneration() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		final int totalDocuments = 100;
		final int totalEmptyFieldDocuments = 50;
		
		for(int i = 0; i < totalDocuments; i++) {
			final String json;
			if(i % 2 == 0) {
				json = "{\"docField\" : \"This is a test\", \"numField\": 123, \"emptyField\": \"\"}";
			} else {
				json = "{\"docField\" : \"This is a test\", \"numField\": 123}";
			}
			
			given()
				.request()
				.body(json)
			.when()
				.post("/" + index + "/" + type)
			.then()
				.statusCode(201);
		}

		final long startTime = System.currentTimeMillis();

		int maxDocValue = 0;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given()
					.request()
					.body("{\"fields\":[\"docField\"]}")
					.when()
					.post("/" + index + "/_field_stats")
					.then();
			try {
				maxDocValue = response.extract().body().jsonPath().getInt("indices." + index + ".fields.docField.max_doc");
				if(maxDocValue < totalDocuments) {
					try {
						Thread.sleep(500);
					} catch (Exception e) {}
					continue;
				}

				given()
						.request()
						.body("{\"fields\":[\"docField\"]}")
						.when()
						.post("/" + index + "/_field_stats")
						.then()
						.statusCode(200)
						.body("_shards.successful", equalTo(1))
						.body("indices." + index + ".fields.docField.max_doc", equalTo(totalDocuments))
						.body("indices." + index + ".fields.docField.doc_count", equalTo(totalDocuments));

				given()
						.request()
						.body("{\"fields\":[\"emptyField\"]}")
						.when()
						.post("/" + index + "/_field_stats")
						.then()
						.statusCode(200)
						.body("_shards.successful", equalTo(1))
						.body("indices." + index + ".fields.emptyField.max_doc", equalTo(totalDocuments))
						.body("indices." + index + ".fields.emptyField.doc_count", equalTo(totalEmptyFieldDocuments));
				return;
			} catch (JsonPathException | NullPointerException e) {}

			try {
				Thread.sleep(500);
			} catch (Exception e) {}
		}

		Assert.fail("Failed to generate stats for " + totalDocuments + " documents - only " + maxDocValue + "/" + totalDocuments);
	}
}
