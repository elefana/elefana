/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import java.util.ArrayList;
import java.util.List;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class DisMaxQuery extends Query {
	private static final String KEY_QUERIES = "queries";
	
	private final List<Query> queries = new ArrayList<Query>();
	
	public DisMaxQuery(Any queryContext) {
		super();
		
		if(!queryContext.get(KEY_QUERIES).valueType().equals(ValueType.INVALID)) {
			for(Any query : queryContext.get(KEY_QUERIES).asList()) {
				queries.add(QueryParser.parseQuery(query));
			}
		}
	}
	
	@Override
	public boolean isMatchAllQuery() {
		for(Query query : queries) {
			if(!query.isMatchAllQuery()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toSqlWhereClause() {
		//TODO: Handle scoring/sorting
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < queries.size(); i++) {
			if(i > 0) {
				result.append(" OR ");
			}
			result.append('(');
			result.append(queries.get(i).toSqlWhereClause());
			result.append(')');
		}
		return result.toString();
	}

}
