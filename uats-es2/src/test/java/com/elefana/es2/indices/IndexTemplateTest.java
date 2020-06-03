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
import static org.hamcrest.Matchers.notNullValue;

import java.util.UUID;

import io.restassured.http.ContentType;
import io.restassured.path.json.exception.JsonPathException;
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
public class IndexTemplateTest {
	private static final long TEST_TIMEOUT = 30000L;

	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test
	public void testIndexTemplate() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";

		given().when().head("/_template/testIndexTemplateA")
				.then()
				.log().all()
				.statusCode(404);
		
		given()
			.request()
			.body("{\"template\": \"" + index + "\",\"mappings\": {\"" + type + "\": {\"_source\": {\"enabled\": false},\"properties\": {\"nonDocField\": {\"type\": \"date\"}}}}}")
		.when()
			.put("/_template/testIndexTemplateA")
		.then()
			.statusCode(200);
		
		given().when().get("/_template/testIndexTemplateA")
			.then()
			.log().all()
			.statusCode(200)
			.body("testIndexTemplateA.template", equalTo(index))
			.body("testIndexTemplateA.mappings." + type, notNullValue());
	}
	
	@Test
	public void testIndexTemplateWithStorageSettings() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"template\": \"" + index + "\",\"storage\":{\"timestamp_path\":\"timeField\" }}")
		.when()
			.put("/_template/testIndexTemplate")
		.then()
			.statusCode(200);
		
		given().when().get("/_template/testIndexTemplate")
			.then()
			.log().all()
			.statusCode(200)
			.body("testIndexTemplate.template", equalTo(index))
			.body("testIndexTemplate.storage.timestamp_path", equalTo("timeField"));
	}

	@Test
	public void testIndexTemplateWithIdIndexDisabled() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body("{\"template\": \"" + index + "\",\"storage\":{\"id_enabled\":false }}")
				.when()
				.put("/_template/testIndexTemplateIdDisabled")
				.then()
				.statusCode(200);

		given().when().get("/_template/testIndexTemplateIdDisabled")
				.then()
				.log().all()
				.statusCode(200)
				.body("testIndexTemplateIdDisabled.template", equalTo(index))
				.body("testIndexTemplateIdDisabled.storage.id_enabled", equalTo(false));

		given()
				.request()
				.contentType(ContentType.JSON)
				.body("{\"message\" : \"This is a message\",\"date\" : \"2017-01-14T14:12:12\"}")
				.when()
				.post("/" + index + "/" + type)
				.then()
				.statusCode(201)
				.body("_index", equalTo(index))
				.body("_type", equalTo(type))
				.body("_id", notNullValue())
				.body("_version", equalTo(1))
				.body("created", equalTo(true));
	}
	
	@Test
	public void testIndexTemplateMappingGeneration() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"template\": \"" + index.substring(0, index.length() - 2) + "*\",\"mappings\": {\"" + type + "\": {\"_source\": {\"enabled\": false},\"properties\": {\"nonDocField\": {\"type\": \"date\"}}}}}")
		.when()
			.put("/_template/testIndexTemplateMappingGeneration")
		.then()
			.statusCode(200);
		
		given()
			.request()
			.body("{\"docField\" : \"This is a test\"}")
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
						index + ".mappings." + type + ".nonDocField.mapping.nonDocField.type");
				if(testValue == null) {
					try {
						Thread.sleep(500L);
					} catch (Exception e) {}
					continue;
				}

				response.body(index + ".mappings." + type + ".nonDocField.mapping.nonDocField.type", equalTo("date"))
						.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));
				return;
			} catch (JsonPathException | NullPointerException e) {}

			try {
				Thread.sleep(500L);
			} catch (Exception e) {}
		}
		Assert.fail("Failed to generate mappings within timeout");
	}
}
