/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class RegexpQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";

	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public RegexpQuery(Any queryContext) {
		super();
		
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				this.value = fieldContext.get(KEY_VALUE).toString();
				
				if(!fieldContext.get(KEY_BOOST).equals(ValueType.INVALID)) {
					this.boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				this.value = fieldContext.toString();
			}
			break;
		}
		this.value = value.replace(".*", "%");
		this.value = value.replace(".", "_");
	}
	
	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' SIMILAR TO '" + value + "'";
	}
}
