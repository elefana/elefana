/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class TypeQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String value;
	protected double boost = 1.0;
	
	public TypeQuery(Any queryContext) {
		super();
		
		value = queryContext.get(KEY_VALUE).toString();
		
		if(!queryContext.get(KEY_BOOST).valueType().equals(ValueType.INVALID)) {
			boost = queryContext.get(KEY_BOOST).toDouble();
		}
	}

	@Override
	public String toSqlWhereClause() {
		return "type = '" + value + "'";
	}

}
