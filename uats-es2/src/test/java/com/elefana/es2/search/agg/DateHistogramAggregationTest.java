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

import java.util.List;
import java.util.UUID;

import com.elefana.TestUtils;
import io.restassured.path.json.exception.JsonPathException;
import io.restassured.response.ValidatableResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class DateHistogramAggregationTest extends AbstractAggregationTest {

	@Test
	public void testDateHistogramAggregationByDays() {
		final long startTime = System.currentTimeMillis();
		List result = null;
		while(System.currentTimeMillis() - startTime < TIMEOUT) {
			final ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY
							+ ", \"aggs\" : {\"aggs_result\" : { \"date_histogram\" : { \"field\" : \"timestamp\", \"interval\": \"day\" }}}}")
					.when()
					.post("/" + INDEX_A + "/_search")
					.then()
					.log().all();

			try {
				result = response.extract().body().jsonPath().getList("aggregations.aggs_result.buckets");
				if(result != null && result.size() == DOCUMENT_QUANTITY) {
					response.statusCode(200);
					Assert.assertNotNull(result.get(0));
					Assert.assertNotNull(result.get(DOCUMENT_QUANTITY - 1));
					return;
				}
			} catch (JsonPathException e) {
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}

		if(result == null) {
			Assert.fail("Expected " + DOCUMENT_QUANTITY + " results but got null list");
		} else {
			Assert.fail("Expected " + DOCUMENT_QUANTITY + " results but got " + result.size());
		}
	}
	
	@Test
	public void testDateHistogramAggregationByDaysWithSubAggregation() {
		final long startTime = System.currentTimeMillis();
		List result = null;
		while(System.currentTimeMillis() - startTime < TIMEOUT) {
			final ValidatableResponse response = given()
					.request()
					.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY
							+ ", \"aggs\" : {\"aggs_result\" : { \"date_histogram\" : { \"field\" : \"timestamp\", \"interval\": \"day\" }, "
							+ "\"aggregations\": {\"subaggs_result\" : { \"sum\" : { \"field\" : \"value\" }}}}}}")
					.when()
					.post("/" + INDEX_A + "/_search")
					.then()
					.log().all();

			try {
				result = response.extract().body().jsonPath().getList("aggregations.aggs_result.buckets");
				if(result != null && result.size() == DOCUMENT_QUANTITY) {
					response.statusCode(200);
					Assert.assertNotNull(result.get(0));
					Assert.assertNotNull(result.get(DOCUMENT_QUANTITY - 1));
					return;
				}
			} catch (JsonPathException e) {
			}

			try {
				Thread.sleep(100);
			} catch (Exception e) {}
		}

		if(result == null) {
			Assert.fail("Expected " + DOCUMENT_QUANTITY + " results but got null list");
		} else {
			Assert.fail("Expected " + DOCUMENT_QUANTITY + " results but got " + result.size());
		}
	}
	
	@Test
	public void testDateHistogramAggregationWithNoResults() {
		final String index = UUID.randomUUID().toString();
		final String type = "test";

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
			.body("aggregations.aggs_result.buckets.size()", equalTo(0));
	}
}
