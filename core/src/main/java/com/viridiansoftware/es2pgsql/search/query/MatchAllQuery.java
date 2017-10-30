/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

public class MatchAllQuery extends Query {
	
	@Override
	public boolean isMatchAllQuery() {
		return true;
	}

	@Override
	public String toSqlWhereClause() {
		return "";
	}

}
