/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.query;

import org.apache.lucene.search.TermQuery;

public class TermQuerySpec extends Es2QuerySpec {
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public TermQuerySpec(TermQuery termQuery) {
		super();
		this.fieldName = termQuery.getTerm().field();
		this.value = termQuery.getTerm().text();
		this.boost = termQuery.getBoost();
	}

	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' = '" + value + "'";
	}
}
