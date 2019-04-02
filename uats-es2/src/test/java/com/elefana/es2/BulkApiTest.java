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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import io.restassured.config.DecoderConfig;
import io.restassured.config.EncoderConfig;
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
	private static final int RANDOM_SEED = 12947358;
	private static final Random RANDOM = new Random(RANDOM_SEED);
	private static final long BULK_INDEX_TIMEOUT = 30000L;
	
	@Before
	public void setup() {
		RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig().defaultContentCharset("UTF-8")).
				decoderConfig(DecoderConfig.decoderConfig().defaultContentCharset("UTF-8"));
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
				validateBulkResponseWithLineBreak(response.extract().asString());
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
				validateBulkResponseWithArabicAndLineBreak(response.extract().asString());
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
				validateBulkResponseWithPipe(response.extract().asString());
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithNullContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithNull(index, type, totalDocuments))
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
				validateBulkResponseWithNull(response.extract().asString());
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithEscapedUnicodeControl() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithEscapedNull(index, type, totalDocuments))
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
				validateBulkResponseWithEscapedNull(response.extract().asString());
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithEmojiContent() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		given()
				.request()
				.body(generateBulkRequestWithEmoji(index, type, totalDocuments))
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
				validateBulkResponseWithEmoji(response.extract().asString());
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
				validateBulkResponseWithCarriageReturn(response.extract().asString());
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
				validateBulkResponseWithCarriageReturnAndLineBreak(response.extract().asString());
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
				validateBulkResponseWithTab(response.extract().asString());
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithDuplicateKey() {
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";

		final String bulkRequest = generateBulkRequestWithFixedId(index, type);
		for(int i = 0; i < 4; i++) {
			given()
					.request()
					.body(bulkRequest)
					.when().
					post("/_bulk")
					.then()
					.statusCode(200)
					.body("errors", equalTo(false))
					.body("items.size()", equalTo(1));
		}

		final long startTime = System.currentTimeMillis();
		int result = 0;

		while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
			ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + 2 + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == 1) {
				return;
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + 1 + " documents, found " + result);
	}
	
	@Test
	public void testBulkIndexingWithInvalidData() {
		final int totalDocuments = RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE * 2);
		final String index = "message-logs-" + UUID.randomUUID().toString();
		final String type = "test";
		
		String data = generateBulkRequest(index, type, totalDocuments);
		data = data.substring(0, data.length() - 5);
		
		given()
			.request().headers("Connection", "keep-alive")
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
				validateBulkResponseWithJsonString(response.extract().asString());
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

	private String generateBulkRequestWithFixedId(String index, String type) {
		StringBuilder result = new StringBuilder();

		result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\", \"_id\" : \"" + 123 + "\" }}\n");
		result.append("{ \"field\" : \"value\" }\n");
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

	private void validateBulkResponseWithJsonString(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("{\"field\":\"{\\\"key\\\":\\\"value\\\"}\"}", docs.get(i).get("_source").toString());
		}
	}

	private String generateBulkRequestWithLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \nline break.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithLineBreak(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a \nline break.", docs.get(i).get("_source").get("field").toString());
		}
	}


	private String generateBulkRequestWithArabicAndLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"" + "مناقشة سبل استخدام يونكود في النظ\r\n القائمة وفيما يخص التطبيقات ال" + "\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithArabicAndLineBreak(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("مناقشة سبل استخدام يونكود في النظ\r\n القائمة وفيما يخص التطبيقات ال", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithPipe(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"مناقشة سبل استخدام يونكود في النظ| القائمة وفيما يخص التطبيقات ال\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithPipe(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("مناقشة سبل استخدام يونكود في النظ| القائمة وفيما يخص التطبيقات ال", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithCarriageReturn(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \rcarriage return.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithCarriageReturn(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a \rcarriage return.", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithCarriageReturnAndLineBreak(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \r\ncarriage return and line break.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithCarriageReturnAndLineBreak(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a \r\ncarriage return and line break.", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithTab(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \ttab.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithTab(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a \ttab.", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithNull(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \u0000null.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithNull(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a null.", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithEmoji(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"\uD83D\uDCE3This has an emoji.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithEmoji(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("\uD83D\uDCE3This has an emoji.", docs.get(i).get("_source").get("field").toString());
		}
	}

	private String generateBulkRequestWithEscapedNull(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"This has a \\u0000null.\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponseWithEscapedNull(String responseBody) {
		final List<Any> docs = JsonIterator.deserialize(responseBody).get("hits").get("hits").asList();
		for(int i = 0; i < docs.size(); i++) {
			Assert.assertEquals("This has a \u0000null.", docs.get(i).get("_source").get("field").toString());
		}
	}
}
