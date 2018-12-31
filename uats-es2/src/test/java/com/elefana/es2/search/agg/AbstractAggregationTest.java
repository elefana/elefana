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
package com.elefana.es2.search.agg;

import static io.restassured.RestAssured.given;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;

import io.restassured.RestAssured;

public class AbstractAggregationTest {
	protected static final int RANDOM_SEED = 963845658;
	protected static final int DOCUMENT_QUANTITY = 100;
	protected static final int [] DOCUMENT_VALUES = generateValues();
	protected static final long TIMEOUT = 60000L;
	
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
