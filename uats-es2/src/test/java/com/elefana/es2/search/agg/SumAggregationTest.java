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
package com.elefana.es2.search.agg;

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
			.body("hits.total", equalTo(DOCUMENT_QUANTITY))
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
		.body("{\"query\":{\"match_all\":{}}, \"size\":" + (DOCUMENT_QUANTITY * 2)
				+ ", \"aggs\" : {\"aggs_result\" : { \"sum\" : { \"field\" : \"value\" }}}}")
		.when()
			.post("/" + indexA + "," + indexB + "/_search")
		.then()
			.statusCode(200)
			.log().all()
			.body("hits.total", equalTo(200))
			.body("aggregations.aggs_result.value", equalTo(expectedSum));
	}
}
