/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.any.Any;

public class ExistsQuery extends Query {
	private static final String KEY_FIELD = "field";
	
	private String fieldName;
	
	public ExistsQuery(Any queryContext) {
		super();
		this.fieldName = queryContext.get(KEY_FIELD).toString();
	}
	
	@Override
	public String toSqlWhereClause() {
		return "data ? '" + fieldName + "'";
	}
}
