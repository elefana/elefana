/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.query;

import org.apache.lucene.search.PhraseQuery;

public class PhraseQuerySpec extends Es2QuerySpec {
	protected String fieldName;
	protected String [] queries;
	protected long slop = 0;
	protected double boost = 1.0;
	
	public PhraseQuerySpec(PhraseQuery query) {
		super();
		fieldName = query.getTerms()[0].field();
		queries = new String[query.getTerms().length];
		
		for(int i = 0; i < query.getTerms().length; i++) {
			queries[i] = query.getTerms()[i].text();
		}
	}

	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		result.append('(');
		for(int i = 0; i < queries.length; i++) {
			if(i > 0) {
				result.append(" OR ");
			}
			result.append("_source->>'" + fieldName + "' LIKE '%" + queries[i] + "%'");
		}
		result.append(')');
		return result.toString();
	}
}
