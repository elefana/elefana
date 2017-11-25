/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.search.query;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.viridiansoftware.elefana.ElefanaApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, classes = { ElefanaApplication.class })
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
