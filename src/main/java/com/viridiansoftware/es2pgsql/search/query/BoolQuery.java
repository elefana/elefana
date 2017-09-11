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
package com.viridiansoftware.es2pgsql.search.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.viridiansoftware.es2pgsql.search.QueryTranslator;

public class BoolQuery extends QueryTranslator {
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
	
	private BoolQueryState state = BoolQueryState.ROOT;
	
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
	
	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		if(startArrayCount == endArrayCount) {
			switch(state) {
			case FILTER:
				filterClauses.add(querySpec);
				break;
			case MUST:
				mustClauses.add(querySpec);
				break;
			case MUST_NOT:
				mustNotClauses.add(querySpec);
				break;
			case SHOULD:
				shouldClauses.add(querySpec);
				break;
			case ROOT:
			default:
				break;
			}
			state = BoolQueryState.ROOT;
			querySpec = null;
		}
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		switch(state) {
		case ROOT:
			switch(name) {
			case KEY_MUST:
				state = BoolQueryState.MUST;
				break;
			case KEY_MUST_NOT:
				state = BoolQueryState.MUST_NOT;
				break;
			case KEY_FILTER:
				state = BoolQueryState.FILTER;
				break;
			case KEY_SHOULD:
				state = BoolQueryState.SHOULD;
				break;
			}
			break;
		case MUST:
		case MUST_NOT:
		case FILTER:
		case SHOULD:
		default:
			super.writeFieldName(name);
			break;
		}
	}
	
	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		super.writeBooleanField(name, value);
		
		if(state != BoolQueryState.ROOT) {
			return;
		}
		switch(name) {
		case KEY_DISABLE_COORD:
			this.disabledCoord = value;
			break;
		case KEY_ADJUST_PURE_NEGATIVE:
			this.adjustPureNegative = value;
			break;
		}
	}
	
	@Override
	public void writeStringField(String name, String value) throws IOException {
		super.writeStringField(name, value);
		
		if(state != BoolQueryState.ROOT) {
			return;
		}
		switch(name) {
		case KEY_MINIMUM_SHOULD_MATCH:
			this.minimumShouldMatch = value;
			break;
		}
	}
	
	private enum BoolQueryState {
		MUST,
		MUST_NOT,
		FILTER,
		SHOULD,
		ROOT
	}
}
