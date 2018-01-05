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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class QueryStringQueryTest extends AbstractQueryTest {

	@Test
	public void testBasicQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"message\",\"query\":\"fox\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(6));
	}
	
	@Test
	public void testPhraseQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"message\",\"query\":\"\\\"fox jumps\\\"\" }}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(4));
	}
	
	@Test
	public void testWildcardQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"status\",\"query\":\"f*ling\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(2));
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"status\",\"query\":\"f??ling\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(2));
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"status\",\"query\":\"f?ling\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(0));
	}
	
	@Test
	public void testMultiFieldQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"size\":100,\"query\":{\"query_string\":{\"fields\":[\"status\",\"message\"],\"query\":\"f*ling the\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(12));
	}
	
	@Test
	public void testInternalDefaultOperatorQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"message\",\"query\":\"(fox jumps)\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(7));
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_field\":\"message\",\"query\":\"fox jumps\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(7));
	}
	
	@Test
	public void testSpecifiedDefaultOperatorQueryString() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_operator\":\"and\",\"default_field\":\"message\",\"query\":\"(fox jumps)\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(4));
		
		given()
			.request()
			.body("{\"query\":{\"query_string\":{\"default_operator\":\"or\",\"default_field\":\"message\",\"query\":\"(fox jumps)\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(7));
	}
}
