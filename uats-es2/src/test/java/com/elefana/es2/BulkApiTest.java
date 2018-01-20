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
package com.elefana.es2;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.util.Random;
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
public class BulkApiTest {
	private static final int RANDOM_SEED = 12947357;
	private static final Random RANDOM = new Random(RANDOM_SEED);
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}

	@Test
	public void testBulkIndexing() {
		final int totalDocuments = RANDOM.nextInt(100);
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body(generateBulkRequest(index, type, totalDocuments))
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(false));
		
		try {
			Thread.sleep(5000L);
		} catch (Exception e) {}
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(totalDocuments));
	}

	@Test
	public void testBulkIndexingTimeSeries() {
		final int totalDocuments = RANDOM.nextInt(100);
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"template\": \"" + index + "\",\"timestamp_path\": \"timestamp\" ,\"mappings\": {}}")
		.when()
			.put("/_template/bulkIndexingTimeSeries")
		.then()
			.statusCode(200);
		
		given()
			.request()
			.body(generateBulkRequest(index, type, totalDocuments))
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(false));
		
		try {
			Thread.sleep(5000L);
		} catch (Exception e) {}
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(totalDocuments));
	}
	
	private String generateBulkRequest(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();
		
		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"value\" }\n");
		}
		return result.toString();
	}
}
