/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.es2.search.agg;

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
public class SumAggregationTest extends AbstractAggregationTest {
	
	@Test
	public void testSumAggregation() {
		int expectedSum = 0;
		for(int i = 0; i < DOCUMENT_VALUES.length; i++) {
			expectedSum += DOCUMENT_VALUES[i];
		}
		
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateDocuments(index, type);
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
					+ ", \"aggs\" : {\"aggs_result\" : { \"sum\" : { \"field\" : \"value\" }}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(100))
			.body("aggregations.aggs_result.value", equalTo(expectedSum));
	}
	
	@Test
	public void testMultiIndexSumAggregation() {
		int expectedSum = 0;
		for(int i = 0; i < DOCUMENT_VALUES.length; i++) {
			expectedSum += DOCUMENT_VALUES[i];
		}
		expectedSum *= 2;
		
		final String indexA = "msumaggtestA";
		final String indexB = "msumaggtestB";
		final String type = "test";
		
		generateDocuments(indexA, type);
		generateDocuments(indexB, type);
		
		given()
		.request()
		.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
				+ ", \"aggs\" : {\"aggs_result\" : { \"sum\" : { \"field\" : \"value\" }}}}")
		.when()
			.post("/" + indexA + "," + indexB + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(100))
			.body("aggregations.aggs_result.value", equalTo(expectedSum));
	}
}
