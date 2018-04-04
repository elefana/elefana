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
package com.elefana.search;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.api.indices.IndexTemplate;
import com.elefana.util.IndexUtils;

public class CitusSearchQueryBuilder implements SearchQueryBuilder {
	private final JdbcTemplate jdbcTemplate;
	private final IndexUtils indexUtils;

	public CitusSearchQueryBuilder(JdbcTemplate jdbcTemplate, IndexUtils indexUtils) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.indexUtils = indexUtils;
	}

	@Override
	public PsqlQueryComponents buildQuery(IndexTemplate matchedIndexTemplate, List<String> indices, String[] types, RequestBodySearch requestBodySearch) {
		if(indices.isEmpty()) {
			final String emptyDataTableName = SEARCH_TABLE_PREFIX + requestBodySearch.hashCode();
			
			final StringBuilder emptyTableQuery = new StringBuilder();
			emptyTableQuery.append("CREATE TEMP TABLE ");
			emptyTableQuery.append(emptyDataTableName);
			emptyTableQuery.append(
					" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, _source jsonb)");
			jdbcTemplate.update(emptyTableQuery.toString());
			
			final PsqlQueryComponents result = new PsqlQueryComponents(emptyDataTableName, "", "", "", "");
			result.getTemporaryTables().add(emptyDataTableName);
			return result;
		}
		
		final StringBuilder whereClause = new StringBuilder();
		if (!requestBodySearch.getQuery().isMatchAllQuery()) {
			whereClause.append("(");
			whereClause.append(requestBodySearch.getQuerySqlWhereClause(matchedIndexTemplate));
			whereClause.append(")");
		}

		if (!IndexUtils.isTypesEmpty(types)) {
			if (!requestBodySearch.getQuery().isMatchAllQuery()) {
				whereClause.append(" AND (");
			} else {
				whereClause.append("(");
			}
			for (int j = 0; j < types.length; j++) {
				if (types[j].length() == 0) {
					continue;
				}
				if (j > 0) {
					whereClause.append(" OR ");
				}
				whereClause.append("_type = '");
				whereClause.append(types[j]);
				whereClause.append("'");
			}
			whereClause.append(")");
		}
		
		final StringBuilder fromComponent = new StringBuilder();
		fromComponent.append('(');
		final String whereResult = whereClause.toString();
		for (int i = 0; i < indices.size(); i++) {
			if (i > 0) {
				fromComponent.append(" UNION ALL");
			}
			fromComponent.append('(');
			fromComponent.append("SELECT * FROM ");
			fromComponent.append(indexUtils.getQueryTarget(indices.get(i)));
			if(!whereResult.isEmpty()) {
				fromComponent.append(" WHERE ");
				fromComponent.append(whereResult);
			}
			fromComponent.append(')');
		}
		fromComponent.append(')');
		
		return new PsqlQueryComponents(fromComponent.toString(), whereClause.toString(), "", requestBodySearch.getQuerySqlOrderClause(), "");
	}

}
