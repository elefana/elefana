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
import static org.hamcrest.CoreMatchers.notNullValue;
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
public class DateHistogramAggregationTest extends AbstractAggregationTest {

	@Test
	public void testDateHistogramAggregationByDays() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateDocuments(index, type);
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
					+ ", \"aggs\" : {\"aggs_result\" : { \"date_histogram\" : { \"field\" : \"timestamp\", \"interval\": \"day\" }}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.body("aggregations.aggs_result.buckets[0]", notNullValue())
			.body("aggregations.aggs_result.buckets[" + (DOCUMENT_QUANTITY - 1) + "]", notNullValue());
	}
	
	@Test
	public void testDateHistogramAggregationByDaysWithSubAggregation() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateDocuments(index, type);
		
		try {
			Thread.sleep(1000L);
		} catch (Exception e) {}
		
		given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
					+ ", \"aggs\" : {\"aggs_result\" : { \"date_histogram\" : { \"field\" : \"timestamp\", \"interval\": \"day\" }, " 
					+ "\"aggregations\": {\"subaggs_result\" : { \"sum\" : { \"field\" : \"value\" }}}}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.log().all()
			.statusCode(200)
			.body("aggregations.aggs_result.buckets[0]", notNullValue())
			.body("aggregations.aggs_result.buckets[" + (DOCUMENT_QUANTITY - 1) + "]", notNullValue());
	}
}
