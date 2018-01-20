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
package com.elefana.es2.search.query;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class MatchAllQueryTest extends AbstractQueryTest {

	@Test
	public void testDefaultToMatchAllQuery() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateTermDocuments(DOCUMENT_QUANTITY, index, type);
		
		given().when()
			.post("/_search")
		.then()
			.log().all()
			.statusCode(200)
			.body("hits.total", equalTo(10));
	}
	
	@Test
	public void testDefaultToMatchAllQueryWithGet() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateTermDocuments(DOCUMENT_QUANTITY, index, type);
		
		given().when()
			.get("/_search")
		.then()
			.log().all()
			.statusCode(200)
			.body("hits.total", equalTo(10));
	}
	
	@Test
	public void testMatchAllQuery() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateTermDocuments(DOCUMENT_QUANTITY, index, type);
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":100}")
		.when()
			.post("/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(100));
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":10}")
		.when()
			.post("/" + index + "/" + type + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(10));
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":100}")
		.when()
			.post("/" + index + "/" + type + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(100));
	}
}
