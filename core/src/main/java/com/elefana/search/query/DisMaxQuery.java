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
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class DisMaxQuery extends Query {
	private static final String KEY_QUERIES = "queries";
	
	private final List<Query> queries = new ArrayList<Query>();
	
	public DisMaxQuery(JsonNode queryContext) throws ElefanaException {
		super();

		if(queryContext.has(KEY_QUERIES) && queryContext.get(KEY_QUERIES).isArray()) {
			final JsonNode queriesNode = queryContext.get(KEY_QUERIES);
			for(int i = 0; i < queriesNode.size(); i++) {
				queries.add(QueryParser.parseQuery(queriesNode.get(i)));
			}
		}
	}
	
	@Override
	public boolean isMatchAllQuery() {
		for(Query query : queries) {
			if(!query.isMatchAllQuery()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toSqlWhereClause(List<String> indices, IndexTemplate indexTemplate,
	                               IndexFieldStatsService indexFieldStatsService) {
		//TODO: Handle scoring/sorting
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < queries.size(); i++) {
			if(i > 0) {
				result.append(" OR ");
			}
			result.append('(');
			result.append(queries.get(i).toSqlWhereClause(indices, indexTemplate, indexFieldStatsService));
			result.append(')');
		}
		return result.toString();
	}

}
