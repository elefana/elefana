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
public class MatchQueryTest extends AbstractQueryTest {

	@Test
	public void testMatchQuery() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generatePhraseDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"match\":{\"message\":\"the\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(10));
		
		given()
			.request()
			.body("{\"query\":{\"match\":{\"message\":\"fox\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(6));
		
		given()
			.request()
			.body("{\"query\":{\"match\":{\"message\":\"fox dog\"}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(9));
	}
}
