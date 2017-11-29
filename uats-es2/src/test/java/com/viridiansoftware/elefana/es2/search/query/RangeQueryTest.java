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
public class RangeQueryTest extends AbstractQueryTest {

	@Test
	public void testRangeQuery() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateRangeDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\": {\"range\" : {\"value\" : {\"gte\" : 1,\"lte\" : 8}}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(8))
			.body("hits.hits[0]._source.value", equalTo(1))
			.body("hits.hits[7]._source.value", equalTo(8));
		
		given()
			.request()
			.body("{\"query\": {\"range\" : {\"value\" : {\"gt\" : 1,\"lt\" : 8}}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(6))
			.body("hits.hits[0]._source.value", equalTo(2))
			.body("hits.hits[5]._source.value", equalTo(7));
	}
}
