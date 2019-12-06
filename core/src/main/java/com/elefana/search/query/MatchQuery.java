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

public class MatchQuery extends Query {
	private static final String KEY_QUERY = "query";
	private static final String KEY_OPERATOR = "operator";
	private static final String KEY_TYPE = "type";
	private static final String KEY_ZERO_TERMS_QUERY = "zero_terms_query";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String query;
	protected String operator = "OR";
	protected String zeroTermsQuery = "NONE";
	protected double boost = 1.0;
	protected MatchMode matchMode = MatchMode.DEFAULT;
	
	public MatchQuery(JsonNode queryContext) {
		super();

		final Iterator<String> fieldNames = queryContext.fieldNames();
		while(fieldNames.hasNext()) {
			final String fieldName = fieldNames.next();
			this.fieldName = fieldName;

			final JsonNode fieldContext = queryContext.get(fieldName);
			if(fieldContext.isObject()) {
				query = fieldContext.get(KEY_QUERY).textValue();
				if(fieldContext.has(KEY_OPERATOR)) {
					operator = fieldContext.get(KEY_OPERATOR).textValue();
				}
				if(fieldContext.has(KEY_ZERO_TERMS_QUERY)) {
					zeroTermsQuery = fieldContext.get(KEY_ZERO_TERMS_QUERY).textValue();
				}
				if(fieldContext.has(KEY_BOOST)) {
					boost = fieldContext.get(KEY_BOOST).asDouble();
				}
				if(fieldContext.has(KEY_TYPE)) {
					matchMode = MatchMode.valueOf(fieldContext.get(KEY_TYPE).textValue().toUpperCase());
				}
			} else {
				query = fieldContext.textValue();
			}
			break;
		}
	}
	
	@Override
	public String toSqlWhereClause(IndexTemplate indexTemplate) {
		switch(matchMode) {
		case PHRASE:
			return toPhraseSqlWhereClause();
		case PHRASE_PREFIX:
			return toPhrasePrefixSqlWhereClause();
		case DEFAULT:
		default:
			return toDefaultSqlWhereClause();
		}
	}
	
	protected String toPhrasePrefixSqlWhereClause() {
		return "_source->>'" + fieldName + "' ILIKE '" + query + "%'";
	}
	
	protected String toPhraseSqlWhereClause() {
		return "_source->>'" + fieldName + "' ILIKE '%" + query + "%'";
	}
		
	protected String toDefaultSqlWhereClause() {
		StringBuilder stringBuilder = new StringBuilder();
		String [] terms = query.split(" ");
		
		stringBuilder.append('(');
		for(int i = 0; i < terms.length; i++) {
			if(i > 0) {
				stringBuilder.append(" " + operator + " ");
			}
			stringBuilder.append("_source->>'" + fieldName + "' ILIKE '%" + terms[i] + "%'");
		}
		stringBuilder.append(')');
		return stringBuilder.toString();
	}
}
