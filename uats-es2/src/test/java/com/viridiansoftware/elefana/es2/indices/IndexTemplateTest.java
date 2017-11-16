/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.indices;

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

import com.viridiansoftware.elefana.ElefanaApplication;

import io.restassured.RestAssured;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, classes = { ElefanaApplication.class })
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
			.statusCode(200)
			.body("template", equalTo(index))
			.body("mappings." + type, notNullValue());
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
			.log().all()
			.body(index + ".mappings." + type + ".nonDocField.mapping.nonDocField.type", equalTo("date"))
			.body(index + ".mappings." + type + ".docField.mapping.docField.type", equalTo("text"));
	}
}
