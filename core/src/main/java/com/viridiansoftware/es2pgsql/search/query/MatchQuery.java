/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class MatchQuery extends Query {
	private static final String KEY_QUERY = "query";
	private static final String KEY_OPERATOR = "operator";
	private static final String KEY_TYPE = "type";
	private static final String KEY_ZERO_TERMS_QUERY = "zero_terms_query";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String query;
	protected String operator = "OR";
	protected String zeroTermsQuery = "NONE";
	protected double boost = 1.0;
	protected MatchMode matchMode = MatchMode.DEFAULT;
	
	public MatchQuery(Any queryContext) {
		super();
		
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				query = fieldContext.get(KEY_QUERY).toString();
				if(fieldContext.get(KEY_OPERATOR).valueType().equals(ValueType.STRING)) {
					operator = fieldContext.get(KEY_OPERATOR).toString();
				}
				if(fieldContext.get(KEY_ZERO_TERMS_QUERY).valueType().equals(ValueType.STRING)) {
					zeroTermsQuery = fieldContext.get(KEY_ZERO_TERMS_QUERY).toString();
				}
				if(fieldContext.get(KEY_BOOST).valueType().equals(ValueType.NUMBER)) {
					boost = fieldContext.get(KEY_BOOST).toDouble();
				}
				if(fieldContext.get(KEY_TYPE).valueType().equals(ValueType.STRING)) {
					matchMode = MatchMode.valueOf(fieldContext.get(KEY_TYPE).toString().toUpperCase());
				}
			} else {
				query = fieldContext.toString();
			}
			break;
		}
	}
	
	@Override
	public String toSqlWhereClause() {
		switch(matchMode) {
		case PHRASE:
			return toPhraseSqlWhereClause();
		case PHRASE_PREFIX:
			return toPhrasePrefixSqlWhereClause();
		case DEFAULT:
		default:
			return toDefaultSqlWhereClause();
		}
	}
	
	protected String toPhrasePrefixSqlWhereClause() {
		return "_source->>'" + fieldName + "' LIKE '" + query + "%'";
	}
	
	protected String toPhraseSqlWhereClause() {
		return "_source->>'" + fieldName + "' LIKE '" + query + "'";
	}
		
	protected String toDefaultSqlWhereClause() {
		StringBuilder stringBuilder = new StringBuilder();
		String [] terms = query.split(" ");
		
		stringBuilder.append('(');
		for(int i = 0; i < terms.length; i++) {
			if(i > 0) {
				stringBuilder.append(" " + operator + " ");
			}
			stringBuilder.append("_source->>'" + fieldName + "' LIKE '%" + terms[i] + "%'");
		}
		stringBuilder.append(')');
		return stringBuilder.toString();
	}
}
