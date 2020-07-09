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
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;
import com.elefana.search.agg.PercentilesAggregation;

import io.restassured.response.ValidatableResponse;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class PercentilesAggregationTest extends AbstractAggregationTest {

	@Test
	public void testPercentilesAggregation() {
		double [] expectedPercentiles = new double[PercentilesAggregation.DEFAULT_PERCENTS.length];
		double [] valuesAsDoubles = new double[DOCUMENT_VALUES.length];
		for(int i = 0; i < DOCUMENT_VALUES.length; i++) {
			valuesAsDoubles[i] = DOCUMENT_VALUES[i];
		}
		
		for(int i = 0; i < expectedPercentiles.length; i++) {
			Percentile percentile = new Percentile(PercentilesAggregation.DEFAULT_PERCENTS[i]).withEstimationType(EstimationType.R_3);
			percentile.setData(valuesAsDoubles);
			expectedPercentiles[i] = percentile.evaluate();
		}
		
		ValidatableResponse response = given()
			.request()
			.body("{\"query\":{\"match_all\":{}}, \"size\":" + DOCUMENT_QUANTITY 
					+ ", \"aggs\" : {\"aggs_result\" : { \"percentiles\" : { \"field\" : \"value\" }}}}")
		.when()
			.post("/" + INDEX_A + "/_search")
		.then()
			.statusCode(200)
			.log().all()
			.body("hits.total", equalTo(DOCUMENT_QUANTITY));
		
		for(int i = 0; i < expectedPercentiles.length; i++) {
			String path = "aggregations.aggs_result.values['" + PercentilesAggregation.DEFAULT_PERCENTS[i] + "']";
			int actual = response.extract().path(path);
			Assert.assertEquals(expectedPercentiles[i], actual, 0.1);
		}
	}
}
