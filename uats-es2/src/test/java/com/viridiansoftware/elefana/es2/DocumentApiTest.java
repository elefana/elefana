/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.viridiansoftware.elefana.ElefanaApplication;

import io.restassured.RestAssured;

import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;

import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class DocumentApiTest {
	private static final String INDEX = UUID.randomUUID().toString();
	private static final String TYPE = "test";
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost:9201";
	}
	
	@Test
	public void testIndexWithoutId() {
		final String message = "This is a test";
		given()
			.request()
			.body("{\"message\" : \"" + message + "\",\"date\" : \"2009-11-15T14:12:12\"}")
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
		indexWithId(id, message);
	}

	@Test
	public void testGet() {
		final String id = UUID.randomUUID().toString();
		final String message = "This is a test at " + System.currentTimeMillis();
		
		indexWithId(id, message);
		
		given().when().get("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(200)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("found", equalTo(true))
			.body("_source.message", equalTo(message))
			.body("_source.date", notNullValue());
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
		indexWithId(id, message);
		
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
			.body("_source.date", notNullValue());
	}

	@Test
	public void testMultiGet() {
		final String id1 = UUID.randomUUID().toString();
		final String id2 = UUID.randomUUID().toString();
		final String message1 = "This is message 1";
		final String message2 = "This is message 2";
		indexWithId(id1, message1);
		indexWithId(id2, message2);
		
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
		indexWithId(id1, message1);
		indexWithId(id2, message2);
		
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
		indexWithId(id1, message1);
		indexWithId(id2, message2);
		
		given()
			.request()
			.body("{\"docs\" : [{\"_id\" : \"" + id1 + "\"}," +
					"{\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/" + INDEX + "/" + TYPE + "/_mget")
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
	public void testBulk() {

	}
	
	private void indexWithId(final String id, final String message) {
		given()
			.request()
			.body("{\"message\" : \"" + message + "\",\"date\" : \"2009-11-15T14:12:12\"}")
		.when()
			.post("/" + INDEX + "/" + TYPE + "/" + id)
		.then()
			.statusCode(201)
			.body("_index", equalTo(INDEX))
			.body("_type", equalTo(TYPE))
			.body("_id", equalTo(id))
			.body("_version", equalTo(1))
			.body("created", equalTo(true));
	}
}
