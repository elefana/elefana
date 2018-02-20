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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.UnsupportedQueryTypeException;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

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
		Any contentContext = JsonIterator.deserialize(content);
		if(!contentContext.get(FIELD_QUERY).valueType().equals(ValueType.INVALID)) {
			return parseQuery(contentContext.get(FIELD_QUERY));
		}
		return new MatchAllQuery();
	}
	
	public static Query parseQuery(Any queryContext) throws ElefanaException {
		if(!queryContext.get(QUERY_BOOL).valueType().equals(ValueType.INVALID)) {
			return new BoolQuery(queryContext.get(QUERY_BOOL));
		} else if(!queryContext.get(QUERY_DIS_MAX).valueType().equals(ValueType.INVALID)) {
			return new DisMaxQuery(queryContext.get(QUERY_DIS_MAX));
		}else if(!queryContext.get(QUERY_EXISTS).valueType().equals(ValueType.INVALID)) {
			return new ExistsQuery(queryContext.get(QUERY_EXISTS));
		} else if(!queryContext.get(QUERY_FILTERED).valueType().equals(ValueType.INVALID)) {
			return new FilteredQuery(queryContext.get(QUERY_FILTERED));
		} else if(!queryContext.get(QUERY_IDS).valueType().equals(ValueType.INVALID)) {
			return new IdsQuery(queryContext.get(QUERY_IDS));
		} else if(!queryContext.get(QUERY_MATCH_ALL).valueType().equals(ValueType.INVALID)) {
			return new MatchAllQuery();
		} else if(!queryContext.get(QUERY_MATCH_PHRASE_PREFIX).valueType().equals(ValueType.INVALID)) {
			return new MatchPhrasePrefixQuery(queryContext.get(QUERY_MATCH_PHRASE_PREFIX));
		} else if(!queryContext.get(QUERY_MATCH_PHRASE).valueType().equals(ValueType.INVALID)) {
			return new MatchPhraseQuery(queryContext.get(QUERY_MATCH_PHRASE));
		} else if(!queryContext.get(QUERY_MATCH).valueType().equals(ValueType.INVALID)) {
			return new MatchQuery(queryContext.get(QUERY_MATCH));
		} else if(!queryContext.get(QUERY_MULTI_MATCH).valueType().equals(ValueType.INVALID)) {
			
		} else if(!queryContext.get(QUERY_PREFIX).valueType().equals(ValueType.INVALID)) {
			return new PrefixQuery(queryContext.get(QUERY_PREFIX));
		} else if(!queryContext.get(QUERY_QUERY_STRING).valueType().equals(ValueType.INVALID)) {
			return new QueryStringQuery(queryContext.get(QUERY_QUERY_STRING));
		} else if(!queryContext.get(QUERY_RANGE).valueType().equals(ValueType.INVALID)) {
			return new RangeQuery(queryContext.get(QUERY_RANGE));
		} else if(!queryContext.get(QUERY_REGEXP).valueType().equals(ValueType.INVALID)) {
			return new RegexpQuery(queryContext.get(QUERY_REGEXP));
		} else if(!queryContext.get(QUERY_TERM).valueType().equals(ValueType.INVALID)) {
			return new TermQuery(queryContext.get(QUERY_TERM));
		} else if(!queryContext.get(QUERY_TYPE).valueType().equals(ValueType.INVALID)) {
			return new TypeQuery(queryContext.get(QUERY_TYPE));
		} else if(!queryContext.get(QUERY_WILDCARD).valueType().equals(ValueType.INVALID)) {
			return new WildcardQuery(queryContext.get(QUERY_WILDCARD));
		}
		
		//Handle nested query contexts
		if(!queryContext.get(FIELD_QUERY).valueType().equals(ValueType.INVALID)) {
			return parseQuery(queryContext.get(FIELD_QUERY));
		}
		
		LOGGER.error("Unsupported query type requested: " + queryContext.toString());
		throw new UnsupportedQueryTypeException();
	}
}
