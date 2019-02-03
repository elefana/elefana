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

import io.restassured.response.ValidatableResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;
import com.elefana.document.psql.PsqlBulkIngestService;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class BulkApiTest {
	private static final int RANDOM_SEED = 12947357;
	private static final Random RANDOM = new Random(RANDOM_SEED);
	private static final long BULK_INDEX_TIMEOUT = 60000L;
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}

	@Test
	public void testBulkIndexing() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body(generateBulkRequest(index, type, totalDocuments))
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(false))
			.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingTimeSeries() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body("{\"template\": \"" + index + "\",\"storage\": { \"timestamp_path\": \"timestamp\" },\"mappings\": {}}")
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
			.body("errors", equalTo(false))
			.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithLineBreakContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithLineBreak(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithArabicAndLineBreakContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithArabicAndLineBreak(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithPipeContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithPipe(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithCarriageReturnContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithCarriageReturn(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithCarriageReturnAndLineBreakContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithCarriageReturnAndLineBreak(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithTabContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithTab(index, type, totalDocuments))
				.when().
				post("/_bulk")
				.then()
				.statusCode(200)
				.body("errors", equalTo(false))
				.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}
	
	@Test
	public void testBulkIndexingWithInvalidData() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		String data = generateBulkRequest(index, type, totalDocuments);
		data = data.substring(0, data.length() - 5);
		
		given()
			.request()
			.body(data)
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(true))
			.body("items.size()", equalTo(totalDocuments));
	}
	
	@Test
	public void testBulkIndexingFormEncodedData() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		final String requestBody = generateBulkRequest(index, type, totalDocuments);
		
		given()
			.request()
			.contentType(ContentType.URLENC)
			.formParam(requestBody, "")
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(false))
			.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}
	
	@Test
	public void testBulkIndexingWithJsonString() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		given()
			.request()
			.body(generateBulkRequestWithJsonString(index, type, totalDocuments))
		.when().
			post("/_bulk")
		.then()
			.statusCode(200)
			.body("errors", equalTo(false))
			.body("items.size()", equalTo(totalDocuments));

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + totalDocuments + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}
	
	private String generateBulkRequest(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();
		
		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"value\" }\n");
		}
		return result.toString();
	}
	
	private String generateBulkRequestWithJsonString(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();
		
		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"{\\\"key\\\":\\\"value\\\"}\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \nline break.\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithArabicAndLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"مناقشة سبل استخدام يونكود في النظ\r\n القائمة وفيما يخص التطبيقات ال\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithPipe(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"مناقشة سبل استخدام يونكود في النظ| القائمة وفيما يخص التطبيقات ال\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithCarriageReturn(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \rcarriage return.\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithCarriageReturnAndLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \r\ncarriage return and line break.\" }\n");
		}
		return result.toString();
	}

	private String generateBulkRequestWithTab(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \ttab.\" }\n");
		}
		return result.toString();
	}
}
