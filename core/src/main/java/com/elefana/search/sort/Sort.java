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
package com.elefana.search.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class Sort {
	private static final String KEY_SORT = "sort";
	private static final String KEY_ORDER = "order";
	private static final String VALUE_ASC = "asc";
	
	private final List<SortClause> clauses = new ArrayList<SortClause>(1);
	
	public String toSqlClause() {
		if(clauses.isEmpty()) {
			return "";
		}
		
		final StringBuilder result = new StringBuilder();
		for(int i = 0; i < clauses.size(); i++) {
			result.append(clauses.get(i).toSqlClause());
			if(i < clauses.size() - 1) {
				result.append(", ");
			}
		}
		return result.toString();
	}
	
	public void parse(Any searchContext) {
		if(!searchContext.get(KEY_SORT).valueType().equals(ValueType.ARRAY)) {
			return;
		}
		for(Any sortClause : searchContext.get(KEY_SORT).asList()) {
			if(sortClause.valueType().equals(ValueType.STRING)) {
				clauses.add(new SortClause(sortClause.toString(), true));
			} else if(sortClause.valueType().equals(ValueType.OBJECT)) {
				final Map<String, Any> sortObject = sortClause.asMap();
				for(String field : sortObject.keySet()) {
					Any fieldSort = sortObject.get(field);
					if(fieldSort.valueType().equals(ValueType.STRING)) {
						clauses.add(new SortClause(field, fieldSort.toString().equalsIgnoreCase(VALUE_ASC)));
					} else if(fieldSort.valueType().equals(ValueType.OBJECT)) {
						Any order = fieldSort.get(KEY_ORDER);
						if(order.valueType().equals(ValueType.STRING)) {
							clauses.add(new SortClause(field, order.toString().equalsIgnoreCase(VALUE_ASC)));
						} else {
							clauses.add(new SortClause(field, true));
						}
					}
				}
			}
		}
	}
}
