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
package com.viridiansoftware.es2pgsql.search;

import org.junit.Test;

import com.viridiansoftware.elefana.search.RequestBodySearch;

public class RequestBodySearchTest {

	@Test
	public void testSearchQueryParsing() throws Exception {
		//final String query = "{\"query\": {\"bool\" : {\"must\" : {\"term\" : { \"user\" : \"kimchy\" }},\"filter\": {\"term\" : { \"tag\" : \"tech\" }},\"must_not\" : {\"range\" : {\"age\" : { \"gte\" : 10, \"lte\" : 20 }}},\"should\" : [{ \"term\" : { \"tag\" : \"wow\" } },{ \"term\" : { \"tag\" : \"elasticsearch\" }}],\"minimum_should_match\" : 1,\"boost\" : 1.0}}}";
		//final String query = "{\"query\": {\"match\" : {\"message\" : \"this is a test\"}}}";
		//final String query = "{\"query\": {\"match_phrase\" : {\"message\" : \"this is a test\"}}}";
		//final String query = "{\"query\": {\"match_phrase_prefix\" : {\"message\" : \"this is a test\"}}}";
		//final String query = "{\"query\": {\"multi_match\" : {\"query\": \"this is a test\", \"fields\": [ \"subject\", \"message\" ] }}}";
		//final String query = "{\"query\": {\"query_string\": {\"query\": \"(content:this OR name:this) AND (content:that OR name:that)\"}}}";
		//final String query = "{\"query\": {\"term\" : { \"user\" : \"Kimchy\" }}}";
		//final String query = "{\"query\": {\"constant_score\" : {\"filter\" : {\"term\" : { \"user\" : \"kimchy\"}},\"boost\" : 1.2}}}";
		//final String query = "{\"query\": {\"range\" : {\"age\" : {\"gte\" : 10,\"lte\" : 20,\"boost\" : 2.0}}}}";
		//final String query = "{\"query\": {\"exists\" : { \"field\" : \"user\" }}}";
		//final String query = "{ \"query\": { \"prefix\" : { \"user\" : \"ki\" }}}";
		//final String query = "{\"query\": {\"type\" : {\"value\" : \"my_type\"}}}";
		//final String query = "{\"query\": {\"ids\" : {\"type\" : \"my_type\",\"values\" : [\"1\", \"4\", \"100\"]}}}";
		final String query = "{\"query\": {\"match_all\": {}},\"aggs\" : {\"price_ranges\" : {\"range\" : {\"field\" : \"price\",\"ranges\" : [{ \"to\" : 50 },{ \"from\" : 50, \"to\" : 100 },{ \"from\" : 100 }]},\"aggs\" : {\"price_stats\" : {\"avg\" : { \"field\" : \"price\" }}}}}}";
		RequestBodySearch requestBodySearch = new RequestBodySearch(query, true);
		System.out.println(requestBodySearch.getQuerySqlWhereClause());
	}
	
	@Test
	public void testRangeAvgAggregation() {
		
	}
}
