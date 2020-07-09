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

import com.elefana.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

import io.restassured.response.ValidatableResponse;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class StatsAggregationTest extends AbstractAggregationTest {

	@Test
	public void testStatsAggregation() {
		float average = 0.0f;
		int sum = 0;
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		
		for(int i = 0; i < DOCUMENT_VALUES.length; i++) {
			average += DOCUMENT_VALUES[i];
			sum += DOCUMENT_VALUES[i];
			
			if(DOCUMENT_VALUES[i] > max) {
				max = DOCUMENT_VALUES[i];
			}
			if(DOCUMENT_VALUES[i] < min) {
				min = DOCUMENT_VALUES[i];
			}
		}
		average /= DOCUMENT_VALUES.length;
		
		ValidatableResponse response = given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
					+ ", \"aggs\" : {\"aggs_result\" : { \"stats\" : { \"field\" : \"value\" }}}}")
		.when()
			.post("/" + INDEX_A + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(DOCUMENT_QUANTITY))
			.body("aggregations.aggs_result.sum", equalTo(sum))
			.body("aggregations.aggs_result.min", equalTo(min))
			.body("aggregations.aggs_result.max", equalTo(max))
			.body("aggregations.aggs_result.count", equalTo(DOCUMENT_VALUES.length));
		
		//Workaround for hamcrest matcher issue
		float actual = response.extract().path("aggregations.aggs_result.avg");
		Assert.assertEquals(average, actual, 0.01f);
	}
}
