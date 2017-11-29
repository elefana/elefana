/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class WildcardQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public WildcardQuery(Any queryContext) {
		super();
		
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				value = fieldContext.get(KEY_VALUE).toString();
				
				if(!fieldContext.get(KEY_BOOST).valueType().equals(ValueType.INVALID)) {
					boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				value = fieldContext.toString();
			}
			break;
		}
	}

	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' LIKE '" + value.replace("*", "%").replace("?", "_") + "'";
	}

}
