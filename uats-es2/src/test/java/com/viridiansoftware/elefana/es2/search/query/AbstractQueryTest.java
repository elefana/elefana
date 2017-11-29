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
	
	protected void generateRangeDocuments(String index, String type) {
		indexDocument(index, type, "{\"value\": 0}");
		indexDocument(index, type, "{\"value\": 1}");
		indexDocument(index, type, "{\"value\": 2}");
		indexDocument(index, type, "{\"value\": 3}");
		indexDocument(index, type, "{\"value\": 4}");
		indexDocument(index, type, "{\"value\": 5}");
		indexDocument(index, type, "{\"value\": 6}");
		indexDocument(index, type, "{\"value\": 7}");
		indexDocument(index, type, "{\"value\": 8}");
		indexDocument(index, type, "{\"value\": 9}");
	}
	
	protected void generatePhraseDocuments(String index, String type) {
		indexDocument(index, type, "{\"message\": \"The quick brown fox jumps over the lazy dog\"}");
		indexDocument(index, type, "{\"message\": \"The quick brown fox\"}");
		indexDocument(index, type, "{\"message\": \"The fox jumps over the lazy dog\"}");
		indexDocument(index, type, "{\"message\": \"The lazy fox jumps over the dog\"}");
		indexDocument(index, type, "{\"message\": \"The dog jumps\"}");
		indexDocument(index, type, "{\"message\": \"The fox jumps\"}");
		indexDocument(index, type, "{\"message\": \"The quiet dog\"}");
		indexDocument(index, type, "{\"message\": \"The underdog\"}");
		indexDocument(index, type, "{\"message\": \"The two tailed fox\"}");
		indexDocument(index, type, "{\"message\": \"The fast hedgehog\"}");
	}
	
	protected void generateTermDocuments(int quantity, String index, String type) {
		for(int i = 0; i < quantity; i++) {
			indexDocument(index, type, "{\"message\" : \"This is sample message " + i + "\",\"date\" : \"2009-11-15T14:12:12\"}");
		}
	}
	
	private void indexDocument(String index, String type, String document) {
		given()
			.request()
			.body(document)
		.when()
			.post("/" + index + "/" + type + "/")
		.then()
			.statusCode(201);
	}
}
