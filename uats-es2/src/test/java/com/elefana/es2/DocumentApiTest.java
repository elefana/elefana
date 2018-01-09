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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;
import com.elefana.document.GetResponse;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;

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
			.contentType(ContentType.JSON)
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
		
		ValidatableResponse response = given()
			.request()
			.body("{\"docs\" : [{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id1 + "\"}," +
					"{\"_index\": \"" + INDEX + "\",\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/_mget")
		.then()
			.statusCode(200);
		
		final List<GetResponse> responses = new ArrayList<GetResponse>();
		for(int i = 0; i < 2; i++) {
			GetResponse getResponse = new GetResponse();
			getResponse.set_index(response.extract().path("docs[" + i + "]._index"));
			getResponse.set_type(response.extract().path("docs[" + i + "]._type"));
			getResponse.set_id(response.extract().path("docs[" + i + "]._id"));
			getResponse.set_version(response.extract().path("docs[" + i + "]._version"));
			getResponse.setFound(response.extract().path("docs[" + i + "].found"));
			responses.add(getResponse);
		}
		for(GetResponse getResponse : responses) {
			Assert.assertEquals(INDEX, getResponse.get_index());
			Assert.assertEquals(TYPE, getResponse.get_type());
			Assert.assertEquals(1, getResponse.get_version());
			Assert.assertEquals(true, getResponse.isFound());
			
			if(getResponse.get_id().equals(id1) || getResponse.get_id().equals(id2)) {
				continue;
			}
			Assert.fail("Mismatch id " + getResponse.get_id());
		}
	}
	
	@Test
	public void testMultiGetWithIndex() {
		final String id1 = UUID.randomUUID().toString();
		final String id2 = UUID.randomUUID().toString();
		final String message1 = "This is message 1";
		final String message2 = "This is message 2";
		indexWithId(id1, message1);
		indexWithId(id2, message2);
		
		ValidatableResponse response = given()
			.request()
			.body("{\"docs\" : [{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id1 + "\"}," +
					"{\"_type\" : \"" + TYPE + "\",\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/" + INDEX + "/_mget")
		.then()
			.statusCode(200);
		
		final List<GetResponse> responses = new ArrayList<GetResponse>();
		for(int i = 0; i < 2; i++) {
			GetResponse getResponse = new GetResponse();
			getResponse.set_index(response.extract().path("docs[" + i + "]._index"));
			getResponse.set_type(response.extract().path("docs[" + i + "]._type"));
			getResponse.set_id(response.extract().path("docs[" + i + "]._id"));
			getResponse.set_version(response.extract().path("docs[" + i + "]._version"));
			getResponse.setFound(response.extract().path("docs[" + i + "].found"));
			responses.add(getResponse);
		}
		for(GetResponse getResponse : responses) {
			Assert.assertEquals(INDEX, getResponse.get_index());
			Assert.assertEquals(TYPE, getResponse.get_type());
			Assert.assertEquals(1, getResponse.get_version());
			Assert.assertEquals(true, getResponse.isFound());
			
			if(getResponse.get_id().equals(id1) || getResponse.get_id().equals(id2)) {
				continue;
			}
			Assert.fail("Mismatch id " + getResponse.get_id());
		}
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
		
		ValidatableResponse response = given()
			.request()
			.body("{\"docs\" : [{\"_id\" : \"" + id1 + "\"}," +
					"{\"_id\" : \"" + id2 + "\"}]}")
		.when()
			.get("/" + INDEX + "/" + TYPE + "/_mget")
		.then()
			.statusCode(200);
		
		final List<GetResponse> responses = new ArrayList<GetResponse>();
		for(int i = 0; i < 2; i++) {
			GetResponse getResponse = new GetResponse();
			getResponse.set_index(response.extract().path("docs[" + i + "]._index"));
			getResponse.set_type(response.extract().path("docs[" + i + "]._type"));
			getResponse.set_id(response.extract().path("docs[" + i + "]._id"));
			getResponse.set_version(response.extract().path("docs[" + i + "]._version"));
			getResponse.setFound(response.extract().path("docs[" + i + "].found"));
			responses.add(getResponse);
		}
		for(GetResponse getResponse : responses) {
			Assert.assertEquals(INDEX, getResponse.get_index());
			Assert.assertEquals(TYPE, getResponse.get_type());
			Assert.assertEquals(1, getResponse.get_version());
			Assert.assertEquals(true, getResponse.isFound());
			
			if(getResponse.get_id().equals(id1) || getResponse.get_id().equals(id2)) {
				continue;
			}
			Assert.fail("Mismatch id " + getResponse.get_id());
		}
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
