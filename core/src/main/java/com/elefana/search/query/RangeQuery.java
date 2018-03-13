/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.search.query;

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

		for(Object fieldKey : queryContext.keys()) {
			final String fieldName = fieldKey.toString();
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
			result.append("elefana_json_field(_source, '" + fieldName + "')::numeric " + (includeLower ? ">=" : ">") + " " + from);
		}
		if (to != null) {
			if (from != null) {
				result.append(" AND ");
			}
			result.append("elefana_json_field(_source, '" + fieldName + "')::numeric " + (includeUpper ? "<=" : "<") + " " + to);
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
