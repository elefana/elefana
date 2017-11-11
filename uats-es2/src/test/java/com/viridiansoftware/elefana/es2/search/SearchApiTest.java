/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.search;

import static io.restassured.RestAssured.given;

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
