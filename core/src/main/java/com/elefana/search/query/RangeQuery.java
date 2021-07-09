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

public class RangeQuery extends Query {
	private static final Logger LOGGER = LoggerFactory.getLogger(RangeQuery.class);
	
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

	public RangeQuery(JsonNode queryContext) {
		super();

		final Iterator<String> fieldNames = queryContext.fieldNames();
		while(fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();
			this.fieldName = fieldName;

			final JsonNode fieldContext = queryContext.get(fieldName);

			if(fieldContext.has(KEY_GTE)) {
				includeLower = true;
				setFrom(fieldContext.get(KEY_GTE));
			} else if(fieldContext.has(KEY_GT)) {
				includeLower = false;
				setFrom(fieldContext.get(KEY_GT));
			}

			if(fieldContext.has(KEY_LTE)) {
				includeUpper = true;
				setTo(fieldContext.get(KEY_LTE));
			} else if(fieldContext.has(KEY_LT)) {
				includeUpper = false;
				setTo(fieldContext.get(KEY_LT));
			}

			if(fieldContext.has(KEY_BOOST)) {
				boost = fieldContext.get(KEY_BOOST).asDouble();
			}
		}
	}

	@Override
	public String toSqlWhereClause(List<String> indices, IndexTemplate indexTemplate, IndexFieldStatsService indexFieldStatsService) {
		StringBuilder result = new StringBuilder();
		
		String column = "";
		
		if(indexTemplate != null && indexTemplate.getStorage().getTimestampPath() != null) {
			if(indexTemplate.getStorage().getTimestampPath().equalsIgnoreCase(fieldName)) {
				column = "_timestamp";
			} else {
				boolean match = false;
				for(String index : indices) {
					if(match) {
						break;
					}
					if(indexFieldStatsService.hasField(index, fieldName) &&
							indexFieldStatsService.isStringField(index, fieldName)) {
						column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
						match = true;
					} else if(indexFieldStatsService.hasField(index, fieldName) &&
							indexFieldStatsService.isDateField(index, fieldName)) {
						column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
						match = true;
					}
				}

				if(!match) {
					if(!fieldName.contains(".")) {
						column = "(_source->>'" + fieldName + "')::numeric";
					} else {
						column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
					}
				}
			}
		} else {
			boolean match = false;
			for(String index : indices) {
				if(match) {
					break;
				}
				if(indexFieldStatsService.hasField(index, fieldName) &&
						indexFieldStatsService.isStringField(index, fieldName)) {
					column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
					match = true;
				} else if(indexFieldStatsService.hasField(index, fieldName) &&
						indexFieldStatsService.isDateField(index, fieldName)) {
					column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
					match = true;
				}
			}

			if(!match) {
				if(!fieldName.contains(".")) {
					column = "(_source->>'" + fieldName + "')::numeric";
				} else {
					column = "elefana_json_field(_source, '" + fieldName + "')::numeric";
				}
			}
		}
		
		if (from != null) {
			result.append(column);
			result.append(' ');
			result.append((includeLower ? ">=" : ">") + " " + from);
		}
		if (to != null) {
			if (from != null) {
				result.append(" AND ");
			}
			result.append(column);
			result.append(' ');
			result.append((includeUpper ? "<=" : "<") + " " + to);
		}
		return result.toString();
	}

	private void setFrom(JsonNode value) {
		from = value.asText();
	}

	private void setTo(JsonNode value) {
		to = value.asText();
	}
}
