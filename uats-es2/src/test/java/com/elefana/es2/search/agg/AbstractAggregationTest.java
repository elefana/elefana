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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.elefana.TestUtils;
import io.restassured.response.ValidatableResponse;
import org.junit.Before;

import io.restassured.RestAssured;
import org.junit.BeforeClass;

public class AbstractAggregationTest {
	protected static final int RANDOM_SEED = 963845658;
	protected static final int DOCUMENT_QUANTITY = 100;
	protected static final int [] DOCUMENT_VALUES = generateValues();
	protected static final long INIT_TIMEOUT = 20000L;
	protected static final long TIMEOUT = 120000L;

	protected static final String INDEX_A = UUID.randomUUID().toString();
	protected static final String INDEX_B = UUID.randomUUID().toString();
	protected static final String TYPE = "test";

	private static final Lock INITIALISE_LOCK = new ReentrantLock();
	private static boolean DATASET_INITIALISED = false;

	protected static void initialiseDataSet() {
		INITIALISE_LOCK.lock();
		if(DATASET_INITIALISED) {
			INITIALISE_LOCK.unlock();
			return;
		}
		try {
			RestAssured.baseURI = "http://localhost:9201";
			TestUtils.waitForElefanaToStart();
			TestUtils.disableMappingAndStatsForIndex(INDEX_A);
			TestUtils.disableMappingAndStatsForIndex(INDEX_B);
			generateDocuments(INDEX_A, TYPE);
			generateDocuments(INDEX_B, TYPE);
			DATASET_INITIALISED = true;
		} finally {
			INITIALISE_LOCK.unlock();
		}
	}
	
	@Before
	public void setup() {
		initialiseDataSet();
		RestAssured.baseURI = "http://localhost:9201";
	}

	private static void generateDocuments(String index, String type) {
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
	
	private static int [] generateValues() {
		int [] result = new int[DOCUMENT_QUANTITY];
		Random random = new Random(RANDOM_SEED);
		for(int i = 0; i < result.length; i++) {
			result[i] = random.nextInt(1000);
		}
		return result;
	}
}
