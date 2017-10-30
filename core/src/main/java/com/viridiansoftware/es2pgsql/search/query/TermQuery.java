/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class TermQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public TermQuery(Any queryContext) {
		super();
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				this.value = fieldContext.get(KEY_VALUE).toString();
				if(fieldContext.keys().contains(KEY_BOOST)) {
					this.boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				this.value = fieldContext.toString();
			}
		}
	}

	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' = '" + value + "'";
	}

}
