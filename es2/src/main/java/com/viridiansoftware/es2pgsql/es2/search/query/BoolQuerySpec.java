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
package com.viridiansoftware.es2pgsql.es2.search.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

import com.viridiansoftware.es2pgsql.search.query.QuerySpec;

public class BoolQuerySpec extends Es2QuerySpec {
	private static final String KEY_MUST = "must";
	private static final String KEY_MUST_NOT = "must_not";
	private static final String KEY_FILTER = "filter";
	private static final String KEY_SHOULD = "should";
	private static final String KEY_DISABLE_COORD = "disable_coord";
	private static final String KEY_ADJUST_PURE_NEGATIVE = "adjust_pure_negative";
	private static final String KEY_MINIMUM_SHOULD_MATCH = "minimum_should_match";
	private static final String KEY_BOOST = "boost";
	
	private final List<QuerySpec> mustClauses = new ArrayList<QuerySpec>();
	private final List<QuerySpec> mustNotClauses = new ArrayList<QuerySpec>();
	private final List<QuerySpec> filterClauses = new ArrayList<QuerySpec>();
	private final List<QuerySpec> shouldClauses = new ArrayList<QuerySpec>();

	private String minimumShouldMatch = "1";
	private double boost = 1.0;
	private boolean disabledCoord = false;
	private boolean adjustPureNegative = true;
	
	public BoolQuerySpec(BooleanQuery booleanQuery) {
		super();
		if(booleanQuery.getMinimumNumberShouldMatch() > 0) {
			minimumShouldMatch = String.valueOf(booleanQuery.getMinimumNumberShouldMatch());
		}
		for(BooleanClause booleanClause : booleanQuery.clauses()) {
			switch(booleanClause.getOccur()) {
			case FILTER:
				filterClauses.add(Es2QuerySpec.parseQuery(booleanClause.getQuery()));
				break;
			case MUST_NOT:
				mustNotClauses.add(Es2QuerySpec.parseQuery(booleanClause.getQuery()));
				break;
			case SHOULD:
				shouldClauses.add(Es2QuerySpec.parseQuery(booleanClause.getQuery()));
				break;
			case MUST:
			default:
				mustClauses.add(Es2QuerySpec.parseQuery(booleanClause.getQuery()));
				break;
			}
		}
	}
	
	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		result.append('(');
		
		if(!mustClauses.isEmpty()) {
			result.append('(');
			for(int i = 0; i < mustClauses.size(); i++) {
				if(i > 0) {
					result.append(" AND ");
				}
				result.append(mustClauses.get(i).toSqlWhereClause());
			}
			result.append(')');
			
			if(!mustNotClauses.isEmpty() || !filterClauses.isEmpty() || !shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!filterClauses.isEmpty()) {
			result.append('(');
			for(int i = 0; i < filterClauses.size(); i++) {
				if(i > 0) {
					result.append(" AND ");
				}
				result.append(filterClauses.get(i).toSqlWhereClause());
			}
			result.append(')');
			
			if(!mustNotClauses.isEmpty() || !shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!mustNotClauses.isEmpty()) {
			result.append("NOT (");
			for(int i = 0; i < mustNotClauses.size(); i++) {
				if(i > 0) {
					result.append(" AND ");
				}
				result.append(mustNotClauses.get(i).toSqlWhereClause());
			}
			result.append(')');
			
			if(!shouldClauses.isEmpty()) {
				result.append(" AND ");
			}
		}
		if(!shouldClauses.isEmpty()) {
			result.append('(');
			
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
					if(i > 0) {
						result.append(" OR ");
					}
					result.append(shouldClauses.get(i).toSqlWhereClause());
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
