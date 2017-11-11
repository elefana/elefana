/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.search.agg;

import static io.restassured.RestAssured.given;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;

import io.restassured.RestAssured;

public class AbstractAggregationTest {
	protected static final int RANDOM_SEED = 963845658;
	protected static final int DOCUMENT_QUANTITY = 100;
	protected static final int [] DOCUMENT_VALUES = generateValues();
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}

	protected void generateDocuments(String index, String type) {
		long timestamp = System.currentTimeMillis() - 1L - (TimeUnit.DAYS.toMillis(DOCUMENT_QUANTITY));
		for(int i = 0; i < DOCUMENT_QUANTITY; i++) {
			given()
				.request()
				.body("{\"value\" : " + DOCUMENT_VALUES[i] + ",\"timestamp\" : " + timestamp + "}")
			.when()
				.post("/" + index + "/" + type + "/")
			.then()
				.statusCode(201);
			timestamp += TimeUnit.DAYS.toMillis(1L);
		}
	}
	
	protected static int [] generateValues() {
		int [] result = new int[DOCUMENT_QUANTITY];
		Random random = new Random(RANDOM_SEED);
		for(int i = 0; i < result.length; i++) {
			result[i] = random.nextInt(1000);
		}
		return result;
	}
}
