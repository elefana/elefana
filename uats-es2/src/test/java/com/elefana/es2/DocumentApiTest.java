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

import com.elefana.DocumentedTest;
import com.elefana.ElefanaApplication;
import com.elefana.TestUtils;
import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.exception.JsonPathException;
import io.restassured.response.ValidatableResponse;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentApiTest extends DocumentedTest {
	private static final String INDEX = UUID.randomUUID().toString();
	private static final String TYPE = "test";
	private static final String MULTI_GET_ID_1 = UUID.randomUUID().toString();
	private static final String MULTI_GET_ID_2 = UUID.randomUUID().toString();

	private static final long TEST_TIMEOUT = 30000L;

	private static final Lock INITIALISE_LOCK = new ReentrantLock();
	private static boolean DATASET_INITIALISED = false;

	private static void initialiseDataset() {
		INITIALISE_LOCK.lock();
		if(DATASET_INITIALISED) {
			INITIALISE_LOCK.unlock();
			return;
		}
		try {
			RestAssured.baseURI = "http://localhost:9201";
			TestUtils.waitForElefanaToStart();
			TestUtils.disableMappingAndStatsForIndex(INDEX);

			final String message1 = "This is message 1";
			final String message2 = "This is message 2";
			indexWithId(MULTI_GET_ID_1, message1, System.currentTimeMillis());
			indexWithId(MULTI_GET_ID_2, message2, System.currentTimeMillis());
			DATASET_INITIALISED = true;
		} finally {
			INITIALISE_LOCK.unlock();
		}
	}

	@Before
	public void setup() {
		initialiseDataset();
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test(timeout = 30000L)
	public void testIndexWithoutId() {
		TestUtils.waitForElefanaToStart();

		final String message = "This is a test";
		given(documentationSpec)
			.request()
			.contentType(ContentType.JSON)
			.body("{\"message\" : \"" + message + "\",\"date\" : \"2017-01-14T14:12:12\"}")
			.filter(createDocumentationFilter("document"))
		.when()
			.post("/" + INDEX + "/" + TYPE)
		.then()
			.statusCode(201)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", notNullValue())
			.body("_version", equalTo(1))
			.body("created", equalTo(true));
	}

	@Test(timeout = 30000L)
	public void testIndexWithId() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		indexWithId(id, message, System.currentTimeMillis());
	}

	@Test(timeout = 30000L)
	public void testExists() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";

		given().when().head("/" + INDEX + "/" + TYPE + "/" + id)
				.then()
				.statusCode(404);

		indexWithId(id, message, System.currentTimeMillis());

		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
				.then()
				.statusCode(200);
	}

	@Test(timeout = 30000L)
	public void testZDelete() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		indexWithId(id, message, System.currentTimeMillis());

		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
				.then()
				.statusCode(200);

		try {
			Thread.sleep(50);
		} catch (Exception e) {}

		given().when().delete("/" + INDEX + "/" + TYPE + "/" + id)
				.then()
				.statusCode(200)
				.body("result", equalTo("deleted"));

		try {
			Thread.sleep(50);
		} catch (Exception e) {}

		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
				.then()
				.statusCode(404);
	}

	@Test(timeout = 30000L)
	public void testZDeleteIndex() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		indexWithId(id, message, System.currentTimeMillis());

		try {
			Thread.sleep(1000);
		} catch (Exception e) {}

		given().when().delete("/" + INDEX)
				.then()
				.statusCode(200);

		final long startTime = System.currentTimeMillis();
		int lastResponseCode = 0;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
					.then().log().all();
			lastResponseCode = response.extract().statusCode();
			if(lastResponseCode == 404) {
				return;
			}
			try {
				Thread.sleep(500);
			} catch (Exception e) {}
		}
		Assert.fail("Expected 404 response but received " + lastResponseCode);
	}
	
	@Test(timeout = 30000L)
	public void testIndexWithEscapedJson() throws IOException {
		TestUtils.waitForElefanaToStart();

		final String document = new Scanner(DocumentApiTest.class.getResource("/escapedSample.json").openStream()).nextLine();
		final String id = UUID.randomUUID().toString();

		given()
			.request()
			.contentType(ContentType.JSON)
			.body(document)
		.when()
			.post("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(201)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", notNullValue())
			.body("_version", equalTo(1))
			.body("created", equalTo(true));
		
		ValidatableResponse response = given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(200)
			.log().all();
		
		JsonNode expectedAny = JsonUtils.extractJsonNode(document);
		JsonNode resultAny = JsonUtils.extractJsonNode(response.extract().asString()).get("_source");
		Assert.assertEquals(expectedAny, resultAny);
		Assert.assertEquals(expectedAny.get("message").toString(), resultAny.get("message").toString());
		
		given()
			.request()
			.contentType(ContentType.JSON)
			.body("{ \"doc\": " + document + "}")
		.when()
			.post("/" + INDEX + "/" + TYPE + "/" + id + "/_update")
		.then()
			.statusCode(202)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", notNullValue());		
		
		response = given()
			.request()
			.body("{\"docs\" : [{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id + "\"}]}")
		.when()
			.post("/_mget")
		.then()
			.log().all()
			.statusCode(200);
		
		resultAny = JsonUtils.extractJsonNode(response.extract().asString()).get("docs").get(0).get("_source");
		Assert.assertEquals(expectedAny, resultAny);
		Assert.assertEquals(expectedAny.get("message").toString(), resultAny.get("message").toString());
	}

	@Test(timeout = 30000L)
	public void testGet() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test at " + System.currentTimeMillis();
		final long timestamp = System.currentTimeMillis();
		
		indexWithId(id, message, timestamp);
		
		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(200)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("found", equalTo(true))
			.body("_source.message", equalTo(message))
			.body("_source.date", notNullValue())
			.body("_source.timestamp", equalTo(timestamp));
	}
	
	@Test(timeout = 30000L)
	public void testGetNotFound() {
		final String id = UUID.randomUUID().toString();
		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(404)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("found", equalTo(false));
	}

	@Test(timeout = 30000L)
	public void testUpdate() {
		TestUtils.waitForElefanaToStart();

		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		final long timestamp = System.currentTimeMillis();
		indexWithId(id, message, timestamp);
		
		final String updatedMessage = "This is an update test";
		
		given()
			.request()
			.body("{\"doc\": {\"message\" : \"" + updatedMessage + "\"}}")
		.when()
			.post("/" + INDEX + "/" + TYPE + "/" + id + "/_update")
		.then()
			.statusCode(202)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("_version", equalTo(1))
			.body("created", equalTo(false));
		
		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(200)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("found", equalTo(true))
			.body("_source.message", equalTo(updatedMessage))
			.body("_source.date", notNullValue())
			.body("_source.timestamp", equalTo(timestamp));
	}

	@Test(timeout = 30000L)
	public void testMultiGet() {
		TestUtils.waitForElefanaToStart();

		final long startTime = System.currentTimeMillis();
		int lastResultCount = 0;
		ValidatableResponse response = null;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			response = given()
					.request()
					.body("{\"docs\" : [{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + MULTI_GET_ID_1 + "\"}," +
							"{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + MULTI_GET_ID_2 + "\"}]}")
					.when()
					.get("/_mget")
					.then();

			try {
				final List docs = response.extract().body().jsonPath().getList("docs");
				lastResultCount = docs.size();
				if (docs.size() > 1) {
					response.body("docs[0]._index", equalTo(INDEX));
					response.body("docs[0]._type", equalTo(TYPE));
					response.body("docs[0]._id", equalTo(MULTI_GET_ID_1));
					response.body("docs[0]._version", equalTo(1));
					response.body("docs[0].found", equalTo(true));
					response.body("docs[1]._index", equalTo(INDEX));
					response.body("docs[1]._type", equalTo(TYPE));
					response.body("docs[1]._id", equalTo(MULTI_GET_ID_2));
					response.body("docs[1]._version", equalTo(1));
					response.body("docs[1].found", equalTo(true));
					return;
				}
			} catch (JsonPathException e) {
			}

			try {
				Thread.sleep(500L);
			} catch (Exception e) {
			}
		}
		response.log().all();
		Assert.fail("Expected 2 results but got " + lastResultCount + " from index " + INDEX);
	}
	
	@Test(timeout = 30000L)
	public void testMultiGetWithIndex() {
		TestUtils.waitForElefanaToStart();

		final long startTime = System.currentTimeMillis();
		int lastResultCount = 0;
		ValidatableResponse response = null;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			response = given()
					.request()
					.body("{\"docs\" : [{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + MULTI_GET_ID_1 + "\"}," +
							"{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + MULTI_GET_ID_2 + "\"}]}")
					.when()
					.get("/" + INDEX + "/_mget")
					.then();

			try {
				final List docs = response.extract().body().jsonPath().getList("docs");
				lastResultCount = docs.size();
				if(docs.size() > 1) {
					response.body("docs[0]._index", equalTo(INDEX));
					response.body("docs[0]._type", equalTo(TYPE));
					response.body("docs[0]._id", equalTo(MULTI_GET_ID_1));
					response.body("docs[0]._version", equalTo(1));
					response.body("docs[0].found", equalTo(true));
					response.body("docs[1]._index", equalTo(INDEX));
					response.body("docs[1]._type", equalTo(TYPE));
					response.body("docs[1]._id", equalTo(MULTI_GET_ID_2));
					response.body("docs[1]._version", equalTo(1));
					response.body("docs[1].found", equalTo(true));
					return;
				}
			} catch (JsonPathException e) {}

			try {
				Thread.sleep(500L);
			} catch (Exception e) {}
		}
		response.log().all();
		Assert.fail("Expected 2 results but got " + lastResultCount + " from index " + INDEX);
	}
	
	@Test(timeout = 30000L)
	public void testMultiGetWithIndexAndType() {
		TestUtils.waitForElefanaToStart();

		final long startTime = System.currentTimeMillis();
		int lastResultCount = 0;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given()
					.request()
					.body("{\"docs\" : [{\"_id\" : \"" + MULTI_GET_ID_1 + "\"}," +
							"{\"_id\" : \"" + MULTI_GET_ID_2 + "\"}]}")
					.when()
					.get("/" + INDEX + "/" + TYPE + "/_mget")
					.then()
					.log().all()
					.statusCode(200);

			try {
				final List docs = response.extract().body().jsonPath().getList("docs");
				lastResultCount = docs.size();
				if(docs.size() > 1) {
					response.body("docs[0]._index", equalTo(INDEX));
					response.body("docs[0]._type", equalTo(TYPE));
					response.body("docs[0]._id", equalTo(MULTI_GET_ID_1));
					response.body("docs[0]._version", equalTo(1));
					response.body("docs[0].found", equalTo(true));
					response.body("docs[1]._index", equalTo(INDEX));
					response.body("docs[1]._type", equalTo(TYPE));
					response.body("docs[1]._id", equalTo(MULTI_GET_ID_2));
					response.body("docs[1]._version", equalTo(1));
					response.body("docs[1].found", equalTo(true));
					return;
				}
			} catch (JsonPathException e) {}

			try {
				Thread.sleep(500L);
			} catch (Exception e) {}
		}
		Assert.fail("Expected 2 results but got " + lastResultCount);
	}
	
	private static void indexWithId(final String id, final String message, final long timestamp) {
		given()
			.request()
			.body("{\"message\" : \"" + message + "\",\"date\" : \"2009-11-15T14:12:12\",\"timestamp\" : " + timestamp + "}")
		.when()
			.post("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.log().all()
			.statusCode(201)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("_version", equalTo(1))
			.body("created", equalTo(true));

		final long startTime = System.currentTimeMillis();
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
					.then();
			if(response.extract().statusCode() == 200) {
				if(response.extract().jsonPath().getBoolean("found")) {
					return;
				}
			}
			try {
				Thread.sleep(500L);
			} catch (Exception e) {}
		}
		Assert.fail("Document was indexed but cannot be retrieved");
	}
}
