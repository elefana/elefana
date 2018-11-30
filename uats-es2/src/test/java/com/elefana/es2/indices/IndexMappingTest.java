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

import io.restassured.response.Response;
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
	private static final long TEST_TIMEOUT = 10000L;

	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
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
		
		try {
			Thread.sleep(3000L);
		} catch (Exception e) {}
		
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
	}
	
	@Test
	public void testIndexRefresh() {
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
		.when()
			.post("/" + index + "/_refresh")
		.then()
			.log().all()
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

		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final Response response = given()
					.request()
					.body("{\"fields\":[\"docField\"]}")
					.when()
					.post("/" + index + "/_field_stats")
					.then().log().all().extract().response();
			final Integer maxDocValue = response.path("indices." + index + ".fields.docField.max_doc");
			if(maxDocValue == null || maxDocValue < totalDocuments) {
				try {
					Thread.sleep(100);
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
					.log().all()
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
					.log().all()
					.body("_shards.successful", equalTo(1))
					.body("indices." + index + ".fields.emptyField.max_doc", equalTo(totalDocuments))
					.body("indices." + index + ".fields.emptyField.doc_count", equalTo(totalEmptyFieldDocuments));
			return;
		}

		Assert.fail("Failed to generate stats for " + totalDocuments + " documents");
	}
}
