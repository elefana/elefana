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
package com.elefana.es2.search.query;

import static io.restassured.RestAssured.given;

import com.elefana.TestUtils;
import io.restassured.response.ValidatableResponse;
import org.junit.Before;

import io.restassured.RestAssured;

import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AbstractQueryTest {
	protected static final int DOCUMENT_QUANTITY = 100;

	protected static final String RANGE_INDEX = UUID.randomUUID().toString();
	protected static final String PHRASE_INDEX = UUID.randomUUID().toString();
	protected static final String TERM_INDEX = UUID.randomUUID().toString();
	protected static final String TYPE = "test";
	protected static final long INIT_TIMEOUT = 20000L;

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
			TestUtils.disableMappingAndStatsForIndex(RANGE_INDEX);
			TestUtils.disableMappingAndStatsForIndex(PHRASE_INDEX);
			TestUtils.disableMappingAndStatsForIndex(TERM_INDEX);
			generateRangeDocuments(RANGE_INDEX, TYPE);
			generatePhraseDocuments(PHRASE_INDEX, TYPE);
			generateTermDocuments(DOCUMENT_QUANTITY, TERM_INDEX, TYPE);
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
	
	private static void generateRangeDocuments(String index, String type) {
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

	private static void generatePhraseDocuments(String index, String type) {
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
		
		indexDocument(index, type, "{\"status\": \"ok\"}");
		indexDocument(index, type, "{\"status\": \"failing\"}");
		indexDocument(index, type, "{\"status\": \"fueling\"}");
	}

	private static void generateTermDocuments(int quantity, String index, String type) {
		for(int i = 0; i < quantity; i++) {
			indexDocument(index, type, "{\"message\" : \"This is sample message " + i + "\",\"date\" : \"2009-11-15T14:12:12\"}");
		}
	}

	private static void indexDocument(String index, String type, String document) {
		given()
			.request()
			.body(document)
		.when()
			.post("/" + index + "/" + type + "/")
		.then()
			.statusCode(201);
	}
}
