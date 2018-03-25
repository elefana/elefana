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

import com.elefana.util.IndexUtils;

/**
 *
 */
public class PartitionTableSearchQueryBuilder implements SearchQueryBuilder {
	private final JdbcTemplate jdbcTemplate;
	private final IndexUtils indexUtils;

	public PartitionTableSearchQueryBuilder(JdbcTemplate jdbcTemplate, IndexUtils indexUtils) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.indexUtils = indexUtils;
	}

	@Override
	public SearchQuery buildQuery(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch) {
		final String queryDataTableName = SEARCH_TABLE_PREFIX + requestBodySearch.hashCode();

		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("CREATE TEMP TABLE ");
		queryBuilder.append(queryDataTableName);
		queryBuilder.append(" AS (");
		
		queryBuilder.append("SELECT * FROM ");
		queryBuilder.append(IndexUtils.DATA_TABLE);
		if(indices.isEmpty()) {
			if (!requestBodySearch.getQuery().isMatchAllQuery() || !IndexUtils.isTypesEmpty(types)) {
				queryBuilder.append(" WHERE TRUE");
			}
		} else {
			queryBuilder.append(" WHERE _index IN (");
			for(int i = 0; i < indices.size(); i++) {
				if(i > 0) {
					queryBuilder.append(',');
				}
				queryBuilder.append("'");
				queryBuilder.append(indices.get(i));
				queryBuilder.append("'");
			}
			queryBuilder.append(") ");
		}
		
		if (!requestBodySearch.getQuery().isMatchAllQuery()) {
			queryBuilder.append(" AND (");
			queryBuilder.append(requestBodySearch.getQuerySqlWhereClause());
			queryBuilder.append(")");
		}
		
		if (!IndexUtils.isTypesEmpty(types)) {
			queryBuilder.append(" AND (");
			for (int j = 0; j < types.length; j++) {
				if (types[j].length() == 0) {
					continue;
				}
				if (j > 0) {
					queryBuilder.append(" OR ");
				}
				queryBuilder.append("_type = '");
				queryBuilder.append(types[j]);
				queryBuilder.append("'");
			}
			queryBuilder.append(")");
		}
		if (requestBodySearch.getSize() > 0) {
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(requestBodySearch.getSize());
		}
		if (requestBodySearch.getFrom() > 0) {
			queryBuilder.append(" OFFSET ");
			queryBuilder.append(requestBodySearch.getFrom());
		}
		queryBuilder.append(")");
		queryBuilder.append(requestBodySearch.getQuerySqlOrderClause());
		jdbcTemplate.update(queryBuilder.toString());
		
		final String query = (requestBodySearch.getSize() == 0 ? "SELECT COUNT(*) " : "SELECT * ") + " FROM " + queryDataTableName;
		final SearchQuery result = new SearchQuery(queryDataTableName, query);
		result.getTemporaryTables().add(queryDataTableName);
		return result;
	}
}
