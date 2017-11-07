/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class FilteredQuery extends Query {
	private static final String KEY_QUERY = "query";
	private static final String KEY_FILTER = "filter";
	
	private Query query, filter;
	
	public FilteredQuery(Any queryContext) {
		super();
		
		if(!queryContext.get(KEY_QUERY).valueType().equals(ValueType.INVALID)) {
			query = QueryParser.parseQuery(queryContext.get(KEY_QUERY));
		} else {
			query = new MatchAllQuery();
		}
		if(!queryContext.get(KEY_FILTER).valueType().equals(ValueType.INVALID)) {
			filter = QueryParser.parseQuery(queryContext.get(KEY_FILTER));
		} else {
			filter = new MatchAllQuery();
		}
	}
	
	@Override
	public boolean isMatchAllQuery() {
		return query.isMatchAllQuery() && filter.isMatchAllQuery();
	}

	@Override
	public String toSqlWhereClause() {
		if(!query.isMatchAllQuery() && !filter.isMatchAllQuery()) {
			StringBuilder result = new StringBuilder();
			result.append('(');
			result.append(query.toSqlWhereClause());
			result.append(" AND ");
			result.append(filter.toSqlWhereClause());
			result.append(')');
			return result.toString();
		} else if(query.isMatchAllQuery()) {
			return filter.toSqlWhereClause();
		} else {
			return query.toSqlWhereClause();
		}
	}

}
