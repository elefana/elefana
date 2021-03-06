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

import java.util.UUID;

import com.elefana.TestUtils;
import org.junit.BeforeClass;
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
public class RegexpQueryTest extends AbstractQueryTest {

	@Test
	public void testRegexpQuery() {
		given()
			.request()
			.body("{\"query\": {\"regexp\":{\"message\": \".*lazy.*\"}}}")
		.when()
			.post("/" + PHRASE_INDEX + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(3));
		
		given()
			.request()
			.body("{\"query\": {\"regexp\":{\"message\": \"the.*\"}}}")
		.when()
			.post("/" + PHRASE_INDEX + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(0));
		
		given()
			.request()
			.body("{\"query\": {\"regexp\":{\"message\": \"[Tt]he.*\"}}}")
		.when()
			.post("/" + PHRASE_INDEX + "/_search")
		.then()
			.statusCode(200)
			.body("hits.total", equalTo(10));
	}
}
