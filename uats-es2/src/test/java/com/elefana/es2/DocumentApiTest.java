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

import static org.springframework.restdocs.operation.preprocess.Preprocessors.preprocessRequest;
import static org.springframework.restdocs.restassured3.RestAssuredRestDocumentation.document;
import static org.springframework.restdocs.restassured3.RestAssuredRestDocumentation.documentationConfiguration;
import static org.springframework.restdocs.restassured3.operation.preprocess.RestAssuredPreprocessors.modifyUris;

import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.restdocs.JUnitRestDocumentation;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.DocumentedTest;
import com.elefana.ElefanaApplication;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class DocumentApiTest extends DocumentedTest {
	private static final String INDEX = UUID.randomUUID().toString();
	private static final String TYPE = "test";
	
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
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
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
	
	private void indexWithId(final String id, final String message) {
		given()
			.request()
			.body("{\"message\" : \"" + message + "\",\"date\" : \"2009-11-15T14:12:12\"}")
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
	}
}
