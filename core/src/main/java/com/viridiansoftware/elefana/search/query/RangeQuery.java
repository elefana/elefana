/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.query;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class RangeQuery extends Query {
	private static final String KEY_GTE = "gte";
	private static final String KEY_GT = "gt";
	private static final String KEY_LTE = "lte";
	private static final String KEY_LT = "lt";
	private static final String KEY_BOOST = "boost";

	protected String fieldName;
	protected String from, to;
	protected boolean includeLower = false;
	protected boolean includeUpper = false;
	protected double boost = 1.0;

	public RangeQuery(Any queryContext) {
		super();

		for (String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;

			Any fieldContext = queryContext.get(fieldName);

			if (!fieldContext.get(KEY_GTE).valueType().equals(ValueType.INVALID)) {
				includeLower = true;
				setFrom(fieldContext.get(KEY_GTE));
			} else if (!fieldContext.get(KEY_GT).valueType().equals(ValueType.INVALID)) {
				includeLower = false;
				setFrom(fieldContext.get(KEY_GT));
			}

			if (!fieldContext.get(KEY_LTE).valueType().equals(ValueType.INVALID)) {
				includeUpper = true;
				setTo(fieldContext.get(KEY_LTE));
			} else if (!fieldContext.get(KEY_LT).valueType().equals(ValueType.INVALID)) {
				includeUpper = false;
				setTo(fieldContext.get(KEY_LT));
			}

			if (!fieldContext.get(KEY_BOOST).valueType().equals(ValueType.INVALID)) {
				boost = fieldContext.get(KEY_BOOST).toDouble();
			}
		}
	}

	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		if (from != null) {
			result.append("(_source->>'" + fieldName + "')::numeric " + (includeLower ? ">=" : ">") + " " + from);
		}
		if (to != null) {
			if (from != null) {
				result.append(" AND ");
			}
			result.append("(_source->>'" + fieldName + "')::numeric " + (includeUpper ? "<=" : "<") + " " + to);
		}
		return result.toString();
	}

	private void setFrom(Any value) {
		from = value.toString();
	}

	private void setTo(Any value) {
		to = value.toString();
	}
}
