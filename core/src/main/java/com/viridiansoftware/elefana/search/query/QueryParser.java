/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import java.util.List;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.viridiansoftware.elefana.exception.UnsupportedQueryTypeException;
import com.viridiansoftware.elefana.search.agg.Aggregation;

public class QueryParser {
	public static final String FIELD_QUERY = "query";
	
	public static final String QUERY_BOOL = "bool";
	public static final String QUERY_EXISTS = "exists";
	public static final String QUERY_IDS = "ids";
	public static final String QUERY_MATCH_ALL = "match_all";
	public static final String QUERY_MATCH_PHRASE_PREFIX = "match_phrase_prefix";
	public static final String QUERY_MATCH_PHRASE = "match_phrase";
	public static final String QUERY_MATCH = "match";
	public static final String QUERY_MULTI_MATCH = "multi_match";
	public static final String QUERY_PREFIX = "prefix";
	public static final String QUERY_RANGE = "range";
	public static final String QUERY_REGEXP = "regexp";
	public static final String QUERY_TERM = "term";
	public static final String QUERY_TYPE = "type";
	public static final String QUERY_WILDCARD = "wildcard";
	
	public static Query parseQuery(String content) {
		Any contentContext = JsonIterator.deserialize(content);
		if(!contentContext.get(FIELD_QUERY).valueType().equals(ValueType.INVALID)) {
			return parseQuery(contentContext.get(FIELD_QUERY));
		}
		return new MatchAllQuery();
	}
	
	public static Query parseQuery(Any queryContext) {
		if(!queryContext.get(QUERY_BOOL).valueType().equals(ValueType.INVALID)) {
			return new BoolQuery(queryContext.get(QUERY_BOOL));
		} else if(!queryContext.get(QUERY_EXISTS).valueType().equals(ValueType.INVALID)) {
			return new ExistsQuery(queryContext.get(QUERY_EXISTS));
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
		} else if(!queryContext.get(QUERY_RANGE).valueType().equals(ValueType.INVALID)) {
			return new RangeQuery(queryContext.get(QUERY_RANGE));
		} else if(!queryContext.get(QUERY_REGEXP).valueType().equals(ValueType.INVALID)) {
			return new RegexpQuery(queryContext.get(QUERY_REGEXP));
		} else if(!queryContext.get(QUERY_TERM).valueType().equals(ValueType.INVALID)) {
			return new TermQuery(queryContext);
		} else if(!queryContext.get(QUERY_TYPE).valueType().equals(ValueType.INVALID)) {
			return new TypeQuery(queryContext.get(QUERY_TYPE));
		} else if(!queryContext.get(QUERY_WILDCARD).valueType().equals(ValueType.INVALID)) {
			return new WildcardQuery(queryContext.get(QUERY_WILDCARD));
		}
		throw new UnsupportedQueryTypeException();
	}
}
