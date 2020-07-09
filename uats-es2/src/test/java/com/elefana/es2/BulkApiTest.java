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

import com.elefana.ElefanaApplication;
import com.elefana.TestUtils;
import com.elefana.api.json.JsonUtils;
import com.elefana.document.psql.PsqlBulkIngestService;
import com.fasterxml.jackson.databind.JsonNode;
import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.config.EncoderConfig;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class BulkApiTest {
	private static final int RANDOM_SEED = 12947358;
	private static final Random RANDOM = new Random(RANDOM_SEED);
	private static final long BULK_INDEX_TIMEOUT = 60000L;
	
	@Before
	public void setup() {
		RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig().defaultContentCharset("UTF-8")).
				decoderConfig(DecoderConfig.decoderConfig().defaultContentCharset("UTF-8"));
		RestAssured.baseURI = "http://localhost:9201";
	}

	@Test
	public void testBulkIndexing() {
		final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + (totalDocuments + 100) + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				response.log().all();
				validateBulkResponse(response.extract().body().asString());
				return;
			}

			try {
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithArabicAndLineBreakContent() {
		final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + (totalDocuments + 100) + "}")
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
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithPipeContent() {
		final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + (totalDocuments + 100) + "}")
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
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	@Test
	public void testBulkIndexingWithDuplicateKey() {
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + 3 + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == 1) {
				return;
			}

			try {
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + 1 + " documents, found " + result);
	}

	@Test
	@Ignore
	public void testBulkIndexingWithDuplicateKeyAllowed() {
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + 3 + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == 4) {
				return;
			}

			try {
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + 4 + " documents, found " + result);
	}
	
	@Test
	public void testBulkIndexingWithInvalidData() {
		final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);
		
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
			.body("items.size()", equalTo(totalDocuments - 1));
	}
	
	@Test
	public void testBulkIndexingFormEncodedData() {
		final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
		final String index = "logs-" + UUID.randomUUID().toString();
		final String type = "test";

		TestUtils.disableMappingAndStatsForIndex(index);

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
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + (totalDocuments + 100) + "}")
					.when()
					.post("/" + index + "/_search")
					.then()
					.statusCode(200);
			result = response.extract().body().jsonPath().getInt("hits.total");
			if(result == totalDocuments) {
				return;
			}

			try {
				Thread.sleep(200);
			} catch (Exception e) {}
		}
		Assert.fail("Expected " + totalDocuments + " documents, found " + result);
	}

	private String generateBulkRequest(String index, String type, int totalDocuments) {
		StringBuilder result = new StringBuilder();

		final long timestamp = System.currentTimeMillis();
		final long timestampFrom = timestamp - (timestamp % TimeUnit.MINUTES.toMillis(1L));
		final long timestampInc = 60000L / totalDocuments;
		
		for(int i = 0; i < totalDocuments; i++) {
			result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
			result.append("{ \"field\" : \"value\", \"specialChars\" : \"This\thas \nspecial\r chars\r\n.\", \"nullField\" : \"This \\u0000has a \u0000null.\", \"emojiField\" : \"\uD83D\uDCE3This has an emoji.\", \"timestamp\": " + (timestampFrom + (timestampInc * i)) + ", \"jsonField\" : \"{\\\"key\\\":\\\"value\\\"}\" }\n");
		}
		return result.toString();
	}

	private void validateBulkResponse(String responseBody) {
		final Iterator<JsonNode> docs =  JsonUtils.extractJsonNode(responseBody).get("hits").get("hits").iterator();
		while(docs.hasNext()) {
			final JsonNode doc = docs.next();
			Assert.assertTrue(doc.get("_source").toString().contains("\"jsonField\":\"{\\\"key\\\":\\\"value\\\"}\""));
			Assert.assertEquals("This\thas \nspecial\r chars\r\n.", doc.get("_source").get("specialChars").textValue());
			Assert.assertEquals("This \\u0000has a null.", doc.get("_source").get("nullField").textValue());
			Assert.assertEquals("\uD83D\uDCE3This has an emoji.", doc.get("_source").get("emojiField").textValue());
		}
	}

	private String generateBulkRequestWithFixedId(String index, String type) {
		StringBuilder result = new StringBuilder();

		result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\", \"_id\" : \"" + 123 + "\" }}\n");
		result.append("{ \"field\" : \"value\" }\n");
		return result.toString();
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
		final Iterator<JsonNode> docs =  JsonUtils.extractJsonNode(responseBody).get("hits").get("hits").iterator();
		while(docs.hasNext()) {
			Assert.assertEquals("مناقشة سبل استخدام يونكود في النظ\r\n القائمة وفيما يخص التطبيقات ال", docs.next().get("_source").get("field").textValue());
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
		final Iterator<JsonNode> docs =  JsonUtils.extractJsonNode(responseBody).get("hits").get("hits").iterator();
		while(docs.hasNext()) {
			Assert.assertEquals("مناقشة سبل استخدام يونكود في النظ| القائمة وفيما يخص التطبيقات ال", docs.next().get("_source").get("field").textValue());
		}
	}
}
