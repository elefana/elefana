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
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;

public class PrefixQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;

	public PrefixQuery(JsonNode queryContext) {
		super();

		final Iterator<String> fieldNames = queryContext.fieldNames();
		while(fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();
			this.fieldName = fieldName;

			final JsonNode fieldContext = queryContext.get(fieldName);
			if(fieldContext.isObject()) {
				value = fieldContext.get(KEY_VALUE).toString();

				if(fieldContext.has(KEY_BOOST) && fieldContext.get(KEY_BOOST).isNumber()) {
					boost = fieldContext.get(KEY_BOOST).asDouble();
				}
			} else {
				value = fieldContext.textValue();
			}
			break;
		}
	}
	
	@Override
	public String toSqlWhereClause(IndexTemplate indexTemplate) {
		return "_source->>'" + fieldName + "' LIKE '" + value + "%'";
	}

}
