/**
 * Copyright 2017 Viridian Software Ltd.
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
 */
package com.viridiansoftware.es2pgsql;

import java.io.IOException;
import java.util.Collections;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment=WebEnvironment.DEFINED_PORT, classes={Es2PgsqlApplication.class})
public class HttpApiControllerTest {
	private RestClient restClient;

	@Before
	public void setUp() {
		restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
	}

	@After
	public void teardown() {
		try {
			restClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIndexApi() throws IOException {
		HttpEntity entity = new NStringEntity("{\n" +
				"    \"user\" : \"kimchy\",\n" + 
				"    \"post_date\" : \"2009-11-15T14:12:12\",\n" + 
				"    \"message\" : \"trying out Elasticsearch\"\n" + "}",
				ContentType.APPLICATION_JSON);
		Response indexResponse = restClient.performRequest("POST", "/twitter/tweet/1",
				Collections.<String, String>emptyMap(), entity);
		System.out.println("RESULT: " + EntityUtils.toString(indexResponse.getEntity()));
	}

	@Test
	public void testGetApi() {

	}

	@Test
	public void testUpdateApi() {

	}

	@Test
	public void testDeleteApi() {

	}

	@Test
	public void testBulkApi() {

	}
}
