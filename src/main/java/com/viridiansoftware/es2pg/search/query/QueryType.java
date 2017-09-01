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
package com.viridiansoftware.es2pg.search.query;

import com.viridiansoftware.es2pg.exception.UnsupportedQueryTypeException;

public enum QueryType {
	MATCH_ALL,
	
	/* FULL TEXT QUERIES */
	MATCH,
	MATCH_PHRASE,
	MATCH_PHRASE_PREFIX,
	MULTI_MATCH,
	COMMON,
	QUERY_STRING,
	SIMPLE_QUERY_STRING,
	
	/* TERM LEVEL QUERIES */
	TERM,
	TERMS,
	RANGE,
	EXISTS,
	PREFIX,
	WILDCARD,
	REGEXP,
	FUZZY,
	TYPE,
	IDS,
	
	/* COMPOUND QUERIES */
	CONSTANT_SCORE,
	BOOL,
	DIS_MAX,
	FUNCTION_SCORE,
	BOOSTING,
	INDICES,
	
	/* JOINING QUERIES */
	NESTED,
	HAS_CHILD,
	HAS_PARENT,
	PARENT_ID,
	
	/* SPECIALISED QUERIES */
	MORE_LIKE_THIS,
	TEMPLATE,
	SCRIPT,
	PERCOLATE,
	
	/* SPAN QUERIES */
	SPAN_TERM,
	SPAN_MULTI,
	SPAN_FIRST,
	SPAN_NEAR,
	SPAN_OR,
	SPAN_NOT,
	SPAN_CONTAINING,
	SPAN_WITHIN
	;
	
	public static QueryType parse(String value) {
		try {
			return QueryType.valueOf(value.toUpperCase());
		} catch (Exception e) {
			throw new UnsupportedQueryTypeException();
		}
	}
}
