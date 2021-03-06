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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class WildcardQuery extends Query {
	private static final Logger LOGGER = LoggerFactory.getLogger(WildcardQuery.class);

	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public WildcardQuery(JsonNode queryContext) {
		super();

		final Iterator<String> fieldNames = queryContext.fieldNames();
		while(fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();
			this.fieldName = fieldName;

			final JsonNode fieldContext = queryContext.get(fieldName);
			if(fieldContext.isObject()) {
				value = fieldContext.get(KEY_VALUE).textValue();

				if(fieldContext.has(KEY_BOOST)) {
					boost = fieldContext.get(KEY_BOOST).asDouble();
				}
			} else {
				value = fieldContext.textValue();
			}
			break;
		}
	}

	@Override
	public String toSqlWhereClause(List<String> indices,  IndexTemplate indexTemplate,
	                               IndexFieldStatsService indexFieldStatsService) {
		if(fieldName.contains(".")) {
			return toProcedureSqlClause();
		}
		try {
			for(String index : indices) {
				if(indexFieldStatsService.hasField(index, fieldName) &&
						indexFieldStatsService.isStringField(index, fieldName)) {
					return "_source->>'" + fieldName + "' LIKE '" + value.replace("*", "%").replace("?", "_") + "'";
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return toProcedureSqlClause();
	}

	private String toProcedureSqlClause() {
		return "elefana_json_field(_source, '" + fieldName + "') LIKE '" + value.replace("*", "%").replace("?", "_") + "'";
	}
}
