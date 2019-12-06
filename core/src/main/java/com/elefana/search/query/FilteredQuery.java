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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.fasterxml.jackson.databind.JsonNode;

public class FilteredQuery extends Query {
	private static final String KEY_QUERY = "query";
	private static final String KEY_FILTER = "filter";
	
	private Query query, filter;
	
	public FilteredQuery(JsonNode queryContext) throws ElefanaException {
		super();

		if(queryContext.has(KEY_QUERY)) {
			query = QueryParser.parseQuery(queryContext.get(KEY_QUERY));
		} else {
			query = new MatchAllQuery();
		}
		if(queryContext.has(KEY_FILTER)) {
			filter = QueryParser.parseQuery(queryContext.get(KEY_FILTER));
		} else {
			filter = new MatchAllQuery();
		}
	}
	
	@Override
	public boolean isMatchAllQuery() {
		return query.isMatchAllQuery() && filter.isMatchAllQuery();
	}

	@Override
	public String toSqlWhereClause(IndexTemplate indexTemplate) {
		if(!query.isMatchAllQuery() && !filter.isMatchAllQuery()) {
			StringBuilder result = new StringBuilder();
			result.append('(');
			result.append(query.toSqlWhereClause(indexTemplate));
			result.append(" AND ");
			result.append(filter.toSqlWhereClause(indexTemplate));
			result.append(')');
			return result.toString();
		} else if(query.isMatchAllQuery()) {
			return filter.toSqlWhereClause(indexTemplate);
		} else {
			return query.toSqlWhereClause(indexTemplate);
		}
	}

}
