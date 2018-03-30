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
package com.elefana.es2.search;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;
import com.elefana.es2.search.query.AbstractQueryTest;

import io.restassured.response.ValidatableResponse;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class SearchApiTest extends AbstractQueryTest {
	
	@Test
	public void testSort() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		final int totalDocuments = 10;
		
		generateRangeDocuments(index, type);
		
		ValidatableResponse response = given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"sort\": [{\"value\": \"desc\"}]}")
		.when()
			.post("/" + index + "/" + type + "/_search")
		.then()
			.log().all()
			.statusCode(200)
			.body("hits.hits.size()", equalTo(totalDocuments));
		
		assertResponseDescending(response);
		
		response = given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"sort\": [{\"value\": {\"order\": \"desc\"}}]}")
		.when()
			.post("/" + index + "/" + type + "/_search")
		.then()
			.log().all()
			.statusCode(200)
			.body("hits.hits.size()", equalTo(totalDocuments));
		
		assertResponseDescending(response);
		
		response = given()
				.request()
				.body("{\"query\":{\"match_all\":{}}, \"sort\": [\"value\"]}")
			.when()
				.post("/" + index + "/" + type + "/_search")
			.then()
				.log().all()
				.statusCode(200)
				.body("hits.hits.size()", equalTo(totalDocuments));
		
		assertResponseAscending(response);
	}
	
	private void assertResponseDescending(ValidatableResponse response) {
		for(int i = 1; i < 10; i++) {
			int previousValue = response.extract().path("hits.hits[" + (i - 1) + "]._source.value");
			int currentValue = response.extract().path("hits.hits[" + i + "]._source.value");
			System.out.println(previousValue + " " + currentValue);
			Assert.assertEquals(true, previousValue > currentValue);
		}
	}
	
	private void assertResponseAscending(ValidatableResponse response) {
		for(int i = 1; i < 10; i++) {
			int previousValue = response.extract().path("hits.hits[" + (i - 1) + "]._source.value");
			int currentValue = response.extract().path("hits.hits[" + i + "]._source.value");
			System.out.println(previousValue + " " + currentValue);
			Assert.assertEquals(true, currentValue > previousValue);
		}
	}
}
