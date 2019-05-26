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
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import io.restassured.path.json.exception.JsonPathException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.DocumentedTest;
import com.elefana.ElefanaApplication;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class DocumentApiTest extends DocumentedTest {
	private static final String INDEX = UUID.randomUUID().toString();
	private static final String TYPE = "test";
	private static final long TEST_TIMEOUT = 30000L;
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test
	public void testIndexWithoutId() {
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

	@Test
	public void testIndexWithId() {
		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		indexWithId(id, message, System.currentTimeMillis());
	}

	@Test
	public void testExists() {
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

	@Test
	public void testDelete() {
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

	@Test
	public void testDeleteIndex() {
		final String id = UUID.randomUUID().toString();
		final String message = "This is a test";
		indexWithId(id, message, System.currentTimeMillis());

		given().when().delete("/" + INDEX)
				.then()
				.statusCode(200);

		final long startTime = System.currentTimeMillis();
		int lastResponseCode = 0;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
					.then();
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
	
	@Test
	public void testIndexWithEscapedJson() throws IOException {
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
		
		Any expectedAny = JsonIterator.deserialize(document);
		Any resultAny = JsonIterator.deserialize(response.extract().asString()).get("_source");
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
		
		resultAny = JsonIterator.deserialize(response.extract().asString()).get("docs").get(0).get("_source");
		Assert.assertEquals(expectedAny, resultAny);
		Assert.assertEquals(expectedAny.get("message").toString(), resultAny.get("message").toString());
	}

	@Test
	public void testGet() {
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
	
	@Test
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

	@Test
	public void testUpdate() {
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

	@Test
	public void testMultiGet() {
		final String id1 = UUID.randomUUID().toString();
		final String id2 = UUID.randomUUID().toString();
		final String message1 = "This is message 1";
		final String message2 = "This is message 2";
		indexWithId(id1, message1, System.currentTimeMillis());
		indexWithId(id2, message2, System.currentTimeMillis());
		
		given()
			.request()
			.body("{\"docs\" : [{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id1 + "\"}," +
					"{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/_mget")
		.then()
			.statusCode(200)
			.body("docs[0]._index", equalTo(INDEX))
			.body("docs[0]._type", equalTo(TYPE))
			.body("docs[0]._id", equalTo(id1))
			.body("docs[0]._version", equalTo(1))
			.body("docs[0].found", equalTo(true))
			.body("docs[1]._index", equalTo(INDEX))
			.body("docs[1]._type", equalTo(TYPE))
			.body("docs[1]._id", equalTo(id2))
			.body("docs[1]._version", equalTo(1))
			.body("docs[1].found", equalTo(true));
	}
	
	@Test
	public void testMultiGetWithIndex() {
		final String id1 = UUID.randomUUID().toString();
		final String id2 = UUID.randomUUID().toString();
		final String message1 = "This is message 1";
		final String message2 = "This is message 2";
		indexWithId(id1, message1, System.currentTimeMillis());
		indexWithId(id2, message2, System.currentTimeMillis());
		
		try {
			Thread.sleep(2000L);
		} catch (Exception e) {}
		
		given()
			.request()
			.body("{\"docs\" : [{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id1 + "\"}," +
					"{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/" + INDEX + "/_mget")
		.then()
			.statusCode(200)
			.body("docs[0]._index", equalTo(INDEX))
			.body("docs[0]._type", equalTo(TYPE))
			.body("docs[0]._id", equalTo(id1))
			.body("docs[0]._version", equalTo(1))
			.body("docs[0].found", equalTo(true))
			.body("docs[1]._index", equalTo(INDEX))
			.body("docs[1]._type", equalTo(TYPE))
			.body("docs[1]._id", equalTo(id2))
			.body("docs[1]._version", equalTo(1))
			.body("docs[1].found", equalTo(true));
	}
	
	@Test
	public void testMultiGetWithIndexAndType() {
		final String id1 = UUID.randomUUID().toString();
		final String id2 = UUID.randomUUID().toString();
		final String message1 = "This is message 1";
		final String message2 = "This is message 2";
		indexWithId(id1, message1, System.currentTimeMillis());
		indexWithId(id2, message2, System.currentTimeMillis());

		final long startTime = System.currentTimeMillis();
		int lastResultCount = 0;
		while(System.currentTimeMillis() - startTime < TEST_TIMEOUT) {
			final ValidatableResponse response = given()
					.request()
					.body("{\"docs\" : [{\"_id\" : \"" + id1 + "\"}," +
							"{\"_id\" : \"" + id2 + "\"}]}")
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
					response.body("docs[0]._id", equalTo(id1));
					response.body("docs[0]._version", equalTo(1));
					response.body("docs[0].found", equalTo(true));
					response.body("docs[1]._index", equalTo(INDEX));
					response.body("docs[1]._type", equalTo(TYPE));
					response.body("docs[1]._id", equalTo(id2));
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
	
	private void indexWithId(final String id, final String message, final long timestamp) {
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
