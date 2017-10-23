/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.query;

public class MatchAllQuerySpec extends Es2QuerySpec {
	
	public boolean isMatchAllQuery() {
		return true;
	}

	@Override
	public String toSqlWhereClause() {
		return "";
	}
}
