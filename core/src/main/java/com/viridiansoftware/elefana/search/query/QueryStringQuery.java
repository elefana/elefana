/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.any.Any;

public class QueryStringQuery extends Query {
	private Query query;
	
	public QueryStringQuery(Any queryContext) {
		super();
		
		//TODO: Query string parser
		query = new MatchAllQuery();
	}
	
	@Override
	public boolean isMatchAllQuery() {
		return query.isMatchAllQuery();
	}

	@Override
	public String toSqlWhereClause() {
		return query.toSqlWhereClause();
	}

}
