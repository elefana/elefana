/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

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
