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
package com.elefana;

public class SearchApiControllerTest {
//	private final ObjectMapper objectMapper = new ObjectMapper();
//	
//	private RestClient restClient;
//
//	@Before
//	public void setUp() throws Exception {
//		restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
//		generateDocuments();
//	}
//
//	@After
//	public void teardown() throws Exception {
//		try {
//			restClient.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Test
//	public void testRangeAvgQuery() throws IOException {
//		final String query = "{\"size\": 0, \"query\": {\"match_all\": {}},\"aggs\" : {\"price_ranges\" : {\"range\" : {\"field\" : \"price\",\"ranges\" : [{ \"to\" : 70 },{ \"from\" : 70, \"to\" : 80 },{ \"from\" : 80 }]},\"aggs\" : {\"price_stats\" : {\"avg\" : { \"field\" : \"price\" }}}}}}";
//		HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
//		Response searchResponse = restClient.performRequest("POST", "/shop/item/_search",
//				Collections.<String, String>emptyMap(), entity);
//		Map<String, Object> result = objectMapper.readValue(EntityUtils.toString(searchResponse.getEntity()), Map.class);
//		System.out.println(result.get("hits"));
//		System.out.println(result.get("aggregations"));
//		System.out.println(getJsonValue(result, "aggregations", "price_ranges"));
//	}
//	
//	private void generateDocuments() throws IOException {
//		for(int i = 50; i < 100; i += 2) {
//			HttpEntity entity = new NStringEntity("{\n" +
//					"    \"price\" : " + i + ",\n" + 
//					"    \"cost\" : " + (i / 5) + "\n" + "}",
//					ContentType.APPLICATION_JSON);
//			restClient.performRequest("POST", "/shop/item/" + i,
//					Collections.<String, String>emptyMap(), entity);
//		}
//	}
//	
//	private Object getJsonValue(Map<String, Object> jsonObject, String... path) {
//		for(int i = 0; i < path.length; i++) {
//			if(!jsonObject.containsKey(path[i])) {
//				return null;
//			}
//			if(i == path.length - 1) {
//				return jsonObject.get(path[i]);
//			}
//			jsonObject = (Map) jsonObject.get(path[i]);
//		}
//		return null;
//	}
}
