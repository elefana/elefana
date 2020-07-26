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

import java.util.List;

public class TypeQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String value;
	protected double boost = 1.0;
	
	public TypeQuery(JsonNode queryContext) {
		super();
		
		value = queryContext.get(KEY_VALUE).textValue();

		if(queryContext.has(KEY_BOOST) && queryContext.get(KEY_BOOST).isNumber()) {
			boost = queryContext.get(KEY_BOOST).asDouble();
		}
	}

	@Override
	public String toSqlWhereClause(List<String> indices, IndexTemplate indexTemplate,
	                               IndexFieldStatsService indexFieldStatsService) {
		return "type = '" + value + "'";
	}

}
