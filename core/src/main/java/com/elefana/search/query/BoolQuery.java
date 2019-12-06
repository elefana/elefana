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
import com.elefana.api.indices.IndexTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BoolQuery extends Query {
	private static final Logger LOGGER = LoggerFactory.getLogger(BoolQuery.class);

	private static final String KEY_MUST = "must";
	private static final String KEY_MUST_NOT = "must_not";
	private static final String KEY_FILTER = "filter";
	private static final String KEY_SHOULD = "should";
	private static final String KEY_DISABLE_COORD = "disable_coord";
	private static final String KEY_ADJUST_PURE_NEGATIVE = "adjust_pure_negative";
	private static final String KEY_MINIMUM_SHOULD_MATCH = "minimum_should_match";
	private static final String KEY_BOOST = "boost";

	private final List<Query> mustClauses = new ArrayList<Query>();
	private final List<Query> mustNotClauses = new ArrayList<Query>();
	private final List<Query> filterClauses = new ArrayList<Query>();
	private final List<Query> shouldClauses = new ArrayList<Query>();
	
	private String minimumShouldMatch = "1";
	private double boost = 1.0;
	private boolean disabledCoord = false;
	private boolean adjustPureNegative = true;

	public BoolQuery(JsonNode queryContext) throws ElefanaException {
		super();

		if(queryContext.has(KEY_MUST)) {
			final JsonNode mustNode = queryContext.get(KEY_MUST);
			if (mustNode.isObject()) {
				mustClauses.add(QueryParser.parseQuery(mustNode));
			} else if (mustNode.isArray()) {
				for(int i = 0; i < mustNode.size(); i++) {
					final JsonNode mustContext = mustNode.get(i);
					mustClauses.add(QueryParser.parseQuery(mustContext));
				}
			}
		}

		if(queryContext.has(KEY_MUST_NOT)) {
			final JsonNode mostNotNode = queryContext.get(KEY_MUST_NOT);
			if (mostNotNode.isObject()) {
				mustNotClauses.add(QueryParser.parseQuery(mostNotNode));
			} else if (mostNotNode.isArray()) {
				for(int i = 0; i < mostNotNode.size(); i++) {
					final JsonNode mustNotContext = mostNotNode.get(i);
					mustNotClauses.add(QueryParser.parseQuery(mustNotContext));
				}
			}
		}

		if(queryContext.has(KEY_FILTER)) {
			final JsonNode filterNode = queryContext.get(KEY_FILTER);
			if (filterNode.isObject()) {
				filterClauses.add(QueryParser.parseQuery(filterNode));
			} else if (filterNode.isArray()) {
				for(int i = 0; i < filterNode.size(); i++) {
					final JsonNode filterContext = filterNode.get(i);
					filterClauses.add(QueryParser.parseQuery(filterContext));
				}
			}
		}

		if(queryContext.has(KEY_SHOULD)) {
			final JsonNode shouldNode = queryContext.get(KEY_SHOULD);
			for(int i = 0; i < shouldNode.size(); i++) {
				final JsonNode shouldContext = shouldNode.get(i);
				shouldClauses.add(QueryParser.parseQuery(shouldContext));
			}
		}

		if(queryContext.has(KEY_MINIMUM_SHOULD_MATCH) && queryContext.get(KEY_MINIMUM_SHOULD_MATCH).isTextual()) {
			this.minimumShouldMatch = queryContext.get(KEY_MINIMUM_SHOULD_MATCH).textValue();
		}
		if(queryContext.has(KEY_BOOST) && queryContext.get(KEY_BOOST).isNumber()) {
			this.boost = queryContext.get(KEY_BOOST).asDouble();
		}
		if(queryContext.has(KEY_DISABLE_COORD) && queryContext.get(KEY_DISABLE_COORD).isBoolean()) {
			this.disabledCoord = queryContext.get(KEY_DISABLE_COORD).asBoolean();
		}
		if(queryContext.has(KEY_ADJUST_PURE_NEGATIVE) && queryContext.get(KEY_ADJUST_PURE_NEGATIVE).isBoolean()) {
			this.adjustPureNegative = queryContext.get(KEY_ADJUST_PURE_NEGATIVE).asBoolean();
		}
	}

	@Override
	public String toSqlWhereClause(IndexTemplate indexTemplate) {
		StringBuilder result = new StringBuilder();
		result.append('(');
		
		if(!mustClauses.isEmpty()) {
			result.append('(');
			int totalClauses = 0;
			for(int i = 0; i < mustClauses.size(); i++) {
				if(mustClauses.get(i).isMatchAllQuery()) {
					continue;
				}
				if(totalClauses > 0) {
					result.append(" AND ");
				}
				result.append(mustClauses.get(i).toSqlWhereClause(indexTemplate));
				totalClauses++;
			}
			result.append(')');
			
			if(!mustNotClauses.isEmpty() || !filterClauses.isEmpty() || !shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!filterClauses.isEmpty()) {
			result.append('(');
			int totalClauses = 0;
			for(int i = 0; i < filterClauses.size(); i++) {
				if(filterClauses.get(i).isMatchAllQuery()) {
					continue;
				}
				if(totalClauses > 0) {
					result.append(" AND ");
				}
				result.append(filterClauses.get(i).toSqlWhereClause(indexTemplate));
				totalClauses++;
			}
			result.append(')');
			
			if(!mustNotClauses.isEmpty() || !shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!mustNotClauses.isEmpty()) {
			result.append("NOT (");
			int totalClauses = 0;
			for(int i = 0; i < mustNotClauses.size(); i++) {
				if(mustNotClauses.get(i).isMatchAllQuery()) {
					continue;
				}
				if(totalClauses > 0) {
					result.append(" AND ");
				}
				result.append(mustNotClauses.get(i).toSqlWhereClause(indexTemplate));
				totalClauses++;
			}
			result.append(')');
			
			if(!shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!shouldClauses.isEmpty()) {
			result.append('(');
			
			int totalClauses = 0;
			int minimumShouldMatch = 1;
			if(this.minimumShouldMatch.contains("%")) {
				float percentage = Float.parseFloat(this.minimumShouldMatch.replace("%", ""));
				if(percentage < 0f) {
					percentage = 100f + percentage;
				}
				minimumShouldMatch = Math.round((percentage / 100f) * shouldClauses.size());
			} else {
				minimumShouldMatch = Integer.parseInt(this.minimumShouldMatch);
			}
			
			switch(minimumShouldMatch) {
			case 1:
				for(int i = 0; i < shouldClauses.size(); i++) {
					if(shouldClauses.get(i).isMatchAllQuery()) {
						continue;
					}
					if(totalClauses > 0) {
						result.append(" OR ");
					}
					result.append(shouldClauses.get(i).toSqlWhereClause(indexTemplate));
					totalClauses++;
				}
				break;
			default:
				//TODO: Handle large sizes
				break;
			}
			result.append(')');
		}
		result.append(')');
		return result.toString();
	}

}
