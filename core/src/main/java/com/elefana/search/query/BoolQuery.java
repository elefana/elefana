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

import java.util.ArrayList;
import java.util.List;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public BoolQuery(Any queryContext) throws ElefanaException {
		super();

		if (queryContext.get(KEY_MUST).valueType().equals(ValueType.OBJECT)) {
			mustClauses.add(QueryParser.parseQuery(queryContext.get(KEY_MUST)));
		} else if (queryContext.get(KEY_MUST).valueType().equals(ValueType.ARRAY)) {
			for(Any mustContext : queryContext.get(KEY_MUST).asList()) {
				mustClauses.add(QueryParser.parseQuery(mustContext));
			}
		}
		
		if (queryContext.get(KEY_MUST_NOT).valueType().equals(ValueType.OBJECT)) {
			mustNotClauses.add(QueryParser.parseQuery(queryContext.get(KEY_MUST_NOT)));
		} else if (queryContext.get(KEY_MUST_NOT).valueType().equals(ValueType.ARRAY)) {
			for(Any mustNotContext : queryContext.get(KEY_MUST_NOT).asList()) {
				mustNotClauses.add(QueryParser.parseQuery(mustNotContext));
			}
		}
		
		if (queryContext.get(KEY_FILTER).valueType().equals(ValueType.OBJECT)) {
			filterClauses.add(QueryParser.parseQuery(queryContext.get(KEY_FILTER)));
		} else if (queryContext.get(KEY_FILTER).valueType().equals(ValueType.ARRAY)) {
			for(Any filterContext : queryContext.get(KEY_FILTER).asList()) {
				filterClauses.add(QueryParser.parseQuery(filterContext));
			}
		}
		
		if (queryContext.get(KEY_SHOULD).valueType().equals(ValueType.ARRAY)) {
			for(Any shouldContext : queryContext.get(KEY_SHOULD).asList()) {
				shouldClauses.add(QueryParser.parseQuery(shouldContext));
			}
		}
		
		if (queryContext.get(KEY_MINIMUM_SHOULD_MATCH).valueType().equals(ValueType.STRING)) {
			this.minimumShouldMatch = queryContext.get(KEY_MINIMUM_SHOULD_MATCH).toString();
		}
		if (queryContext.get(KEY_BOOST).valueType().equals(ValueType.NUMBER)) {
			this.boost = queryContext.get(KEY_BOOST).toDouble();
		}
		if (queryContext.get(KEY_DISABLE_COORD).valueType().equals(ValueType.BOOLEAN)) {
			this.disabledCoord = queryContext.get(KEY_DISABLE_COORD).toBoolean();
		}
		if (queryContext.get(KEY_ADJUST_PURE_NEGATIVE).valueType().equals(ValueType.BOOLEAN)) {
			this.adjustPureNegative = queryContext.get(KEY_ADJUST_PURE_NEGATIVE).toBoolean();
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
