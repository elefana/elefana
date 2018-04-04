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

	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test
	public void testIndexTemplate() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"template\": \"" + index + "\",\"mappings\": {\"" + type + "\": {\"_source\": {\"enabled\": false},\"properties\": {\"nonDocField\": {\"type\": \"date\"}}}}}")
		.when()
			.put("/_template/testIndexTemplate")
		.then()
			.statusCode(200);
		
		given().when().get("/_template/testIndexTemplate")
			.then()
			.log().all()
			.statusCode(200)
			.body("testIndexTemplate.template", equalTo(index))
			.body("testIndexTemplate.mappings." + type, notNullValue());
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
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
		given().when().get("/" + index + "/_mapping/" + type)
		.then()
			.statusCode(200)
			.body(index + ".mappings." + type + ".nonDocField.mapping.nonDocField.type", equalTo("date"))
			.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));
	}
}
