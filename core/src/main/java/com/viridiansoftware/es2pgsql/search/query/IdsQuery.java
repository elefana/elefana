/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import java.util.ArrayList;
import java.util.List;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class IdsQuery extends Query {
	private static final String KEY_TYPE = "type";
	private static final String KEY_VALUES = "values";
	private static final String KEY_BOOST = "boost";
	
	protected final List<String> types = new ArrayList<String>();
	protected final List<String> values = new ArrayList<String>();
	protected double boost = 1.0;
	
	public IdsQuery(Any queryContext) {
		super();
		
		Any typeContext = queryContext.get(KEY_TYPE);
		if(typeContext.valueType().equals(ValueType.ARRAY)) {
			for(Any type : typeContext.asList()) {
				types.add(type.toString());
			}
		} else if(typeContext.valueType().equals(ValueType.STRING)) {
			types.add(typeContext.toString());
		}
		
		Any valuesContext = queryContext.get(KEY_VALUES);
		if(valuesContext.valueType().equals(ValueType.ARRAY)) {
			for(Any value : valuesContext.asList()) {
				values.add(value.toString());
			}
		} else if(valuesContext.valueType().equals(ValueType.STRING)) {
			values.add(valuesContext.toString());
		}
		
		if(queryContext.get(KEY_BOOST).valueType().equals(ValueType.NUMBER)) {
			boost = queryContext.get(KEY_BOOST).toDouble();
		}
	}
	
	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		if(!types.isEmpty()) {
			result.append('(');
			for(int i = 0; i < types.size(); i++) {
				if(i > 0) {
					result.append(" OR ");
				}
				result.append("type = '");
				result.append(types.get(i));
				result.append("'");
			}
			result.append(')');
			if(!values.isEmpty()) {
				result.append(" AND ");
			}
		}
		result.append('(');
		for(int i = 0; i < values.size(); i++) {
			if(i > 0) {
				result.append(" OR ");
			}
			result.append("id = '");
			result.append(values.get(i));
			result.append("'");
		}
		result.append(')');
		return result.toString();
	}
}
