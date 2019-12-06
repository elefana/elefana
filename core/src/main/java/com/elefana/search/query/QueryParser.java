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
package com.elefana.search.query;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.UnsupportedQueryTypeException;
import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryParser {
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
	
	public static final String FIELD_QUERY = "query";
	
	public static final String QUERY_BOOL = "bool";
	public static final String QUERY_DIS_MAX = "dis_max";
	public static final String QUERY_EXISTS = "exists";
	public static final String QUERY_FILTERED = "filtered";
	public static final String QUERY_IDS = "ids";
	public static final String QUERY_MATCH_ALL = "match_all";
	public static final String QUERY_MATCH_PHRASE_PREFIX = "match_phrase_prefix";
	public static final String QUERY_MATCH_PHRASE = "match_phrase";
	public static final String QUERY_MATCH = "match";
	public static final String QUERY_MULTI_MATCH = "multi_match";
	public static final String QUERY_PREFIX = "prefix";
	public static final String QUERY_QUERY_STRING = "query_string";
	public static final String QUERY_RANGE = "range";
	public static final String QUERY_REGEXP = "regexp";
	public static final String QUERY_TERM = "term";
	public static final String QUERY_TYPE = "type";
	public static final String QUERY_WILDCARD = "wildcard";
	
	public static Query parseQuery(String content) throws ElefanaException {
		if(content == null || content.isEmpty()) {
			return new MatchAllQuery();
		}
		final JsonNode contentContext = JsonUtils.extractJsonNode(content);
		if(contentContext.has(FIELD_QUERY)) {
			return parseQuery(contentContext.get(FIELD_QUERY));
		}
		return new MatchAllQuery();
	}
	
	public static Query parseQuery(JsonNode queryContext) throws ElefanaException {
		if(queryContext.has(QUERY_BOOL)) {
			return new BoolQuery(queryContext.get(QUERY_BOOL));
		} else if(queryContext.has(QUERY_DIS_MAX)) {
			return new DisMaxQuery(queryContext.get(QUERY_DIS_MAX));
		} else if(queryContext.has(QUERY_EXISTS)) {
			return new ExistsQuery(queryContext.get(QUERY_EXISTS));
		} else if(queryContext.has(QUERY_FILTERED)) {
			return new FilteredQuery(queryContext.get(QUERY_FILTERED));
		} else if(queryContext.has(QUERY_IDS)) {
			return new IdsQuery(queryContext.get(QUERY_IDS));
		} else if(queryContext.has(QUERY_MATCH_ALL)) {
			return new MatchAllQuery();
		} else if(queryContext.has(QUERY_MATCH_PHRASE_PREFIX)) {
			return new MatchPhrasePrefixQuery(queryContext.get(QUERY_MATCH_PHRASE_PREFIX));
		} else if(queryContext.has(QUERY_MATCH_PHRASE)) {
			return new MatchPhraseQuery(queryContext.get(QUERY_MATCH_PHRASE));
		} else if(queryContext.has(QUERY_MATCH)) {
			return new MatchQuery(queryContext.get(QUERY_MATCH));
		} else if(queryContext.has(QUERY_MULTI_MATCH)) {
			
		} else if(queryContext.has(QUERY_PREFIX)) {
			return new PrefixQuery(queryContext.get(QUERY_PREFIX));
		} else if(queryContext.has(QUERY_QUERY_STRING)) {
			return new QueryStringQuery(queryContext.get(QUERY_QUERY_STRING));
		} else if(queryContext.has(QUERY_RANGE)) {
			return new RangeQuery(queryContext.get(QUERY_RANGE));
		} else if(queryContext.has(QUERY_REGEXP)) {
			return new RegexpQuery(queryContext.get(QUERY_REGEXP));
		} else if(queryContext.has(QUERY_TERM)) {
			return new TermQuery(queryContext.get(QUERY_TERM));
		} else if(queryContext.has(QUERY_TYPE)) {
			return new TypeQuery(queryContext.get(QUERY_TYPE));
		} else if(queryContext.has(QUERY_WILDCARD)) {
			return new WildcardQuery(queryContext.get(QUERY_WILDCARD));
		}
		
		//Handle nested query contexts
		if(queryContext.has(FIELD_QUERY)) {
			return parseQuery(queryContext.get(FIELD_QUERY));
		}
		
		LOGGER.error("Unsupported query type requested: " + queryContext.toString());
		throw new UnsupportedQueryTypeException(queryContext.toString());
	}
}
