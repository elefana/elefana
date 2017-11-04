/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

public abstract class Query {

	public boolean isMatchAllQuery() {
		return false;
	}
	
	public abstract String toSqlWhereClause();
}
