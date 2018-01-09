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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.elefana.ElefanaApplication;

import io.restassured.response.ValidatableResponse;


@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class RangeQueryTest extends AbstractQueryTest {

	@Test
	public void testRangeQuery() {
		final List<Integer> returnedValues = new ArrayList<Integer>();
		
		final String index = UUID.randomUUID().toString();
		final String type = "test";
		
		generateRangeDocuments(index, type);
		
		ValidatableResponse response = given()
			.request()
			.body("{\"query\": {\"range\" : {\"value\" : {\"gte\" : 1,\"lte\" : 8}}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.log().all()
			.body("hits.total", equalTo(8));
		
		for(int i = 0; i < 8; i++) {
			returnedValues.add(response.extract().path("hits.hits[" + i + "]._source.value"));
		}
		Collections.sort(returnedValues);
		Assert.assertEquals(1, (int) returnedValues.get(0));
		Assert.assertEquals(8, (int) returnedValues.get(7));
		returnedValues.clear();
		
		response = given()
			.request()
			.body("{\"query\": {\"range\" : {\"value\" : {\"gt\" : 1,\"lt\" : 8}}}}")
		.when()
			.post("/" + index + "/_search")
		.then()
			.statusCode(200)
			.log().all()
			.body("hits.total", equalTo(6));
		
		for(int i = 0; i < 6; i++) {
			returnedValues.add(response.extract().path("hits.hits[" + i + "]._source.value"));
		}
		Collections.sort(returnedValues);
		Assert.assertEquals(2, (int) returnedValues.get(0));
		Assert.assertEquals(7, (int) returnedValues.get(5));
		returnedValues.clear();
	}
}
