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

import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class IdsQuery extends Query {
	private static final String KEY_TYPE = "type";
	private static final String KEY_VALUES = "values";
	private static final String KEY_BOOST = "boost";
	
	protected final List<String> types = new ArrayList<String>();
	protected final List<String> values = new ArrayList<String>();
	protected double boost = 1.0;
	
	public IdsQuery(JsonNode queryContext) {
		super();

		if(queryContext.has(KEY_TYPE)) {
			final JsonNode typeContext = queryContext.get(KEY_TYPE);
			if(typeContext.isArray()) {
				for(int i = 0; i < typeContext.size(); i++) {
					types.add(typeContext.get(i).textValue());
				}
			} else if(typeContext.isTextual()) {
				types.add(typeContext.toString());
			}
		}

		if(queryContext.has(KEY_VALUES)) {
			final JsonNode valuesContext = queryContext.get(KEY_VALUES);
			if(valuesContext.isArray()) {
				for(int i = 0; i < valuesContext.size(); i++) {
					values.add(valuesContext.get(i).textValue());
				}
			} else if(valuesContext.isTextual()) {
				values.add(valuesContext.toString());
			}
		}

		if(queryContext.has(KEY_BOOST) && queryContext.get(KEY_BOOST).isNumber()) {
			boost = queryContext.get(KEY_BOOST).asDouble();
		}
	}
	
	@Override
	public String toSqlWhereClause(List<String> indices, IndexTemplate indexTemplate,
	                               IndexFieldStatsService indexFieldStatsService) {
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
