/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment=WebEnvironment.DEFINED_PORT, classes={Es2PgsqlApplication.class})
@Transactional
public class SearchApiControllerTest {
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	private RestClient restClient;

	@Before
	public void setUp() throws Exception {
		restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
		generateDocuments();
	}

	@After
	public void teardown() throws Exception {
		try {
			restClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRangeAvgQuery() throws IOException {
		final String query = "{\"size\": 0, \"query\": {\"match_all\": {}},\"aggs\" : {\"price_ranges\" : {\"range\" : {\"field\" : \"price\",\"ranges\" : [{ \"to\" : 70 },{ \"from\" : 70, \"to\" : 80 },{ \"from\" : 80 }]},\"aggs\" : {\"price_stats\" : {\"avg\" : { \"field\" : \"price\" }}}}}}";
		HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
		Response searchResponse = restClient.performRequest("POST", "/shop/item/_search",
				Collections.<String, String>emptyMap(), entity);
		Map<String, Object> result = objectMapper.readValue(EntityUtils.toString(searchResponse.getEntity()), Map.class);
		System.out.println(result.get("hits"));
		System.out.println(result.get("aggregations"));
		System.out.println(getJsonValue(result, "aggregations", "price_ranges"));
	}
	
	private void generateDocuments() throws IOException {
		for(int i = 50; i < 100; i += 2) {
			HttpEntity entity = new NStringEntity("{\n" +
					"    \"price\" : " + i + ",\n" + 
					"    \"cost\" : " + (i / 5) + "\n" + "}",
					ContentType.APPLICATION_JSON);
			restClient.performRequest("POST", "/shop/item/" + i,
					Collections.<String, String>emptyMap(), entity);
		}
	}
	
	private Object getJsonValue(Map<String, Object> jsonObject, String... path) {
		for(int i = 0; i < path.length; i++) {
			if(!jsonObject.containsKey(path[i])) {
				return null;
			}
			if(i == path.length - 1) {
				return jsonObject.get(path[i]);
			}
			jsonObject = (Map) jsonObject.get(path[i]);
		}
		return null;
	}
}
