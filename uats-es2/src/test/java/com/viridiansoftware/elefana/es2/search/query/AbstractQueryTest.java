/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.search.query;

import static io.restassured.RestAssured.given;

import org.junit.Before;

import io.restassured.RestAssured;

public class AbstractQueryTest {
	protected static final int DOCUMENT_QUANTITY = 100;
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	protected void generateDocuments(int quantity, String index, String type) {
		for(int i = 0; i < quantity; i++) {
			given()
				.request()
				.body("{\"message\" : \"This is sample message " + i + "\",\"date\" : \"2009-11-15T14:12:12\"}")
			.when()
				.post("/" + index + "/" + type + "/")
			.then()
				.statusCode(201);
		}
	}
}
