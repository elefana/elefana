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

import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.elefana.util.CumulativeAverage;
import com.elefana.util.IndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 *
 */
public class PartitionTableSearchQueryBuilder implements SearchQueryBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchQueryBuilder.class);

	private final JdbcTemplate jdbcTemplate;
	private final IndexUtils indexUtils;
	private final IndexFieldStatsService indexFieldStatsService;

	public PartitionTableSearchQueryBuilder(JdbcTemplate jdbcTemplate, IndexUtils indexUtils, IndexFieldStatsService indexFieldStatsService) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.indexUtils = indexUtils;
		this.indexFieldStatsService = indexFieldStatsService;
	}

	@Override
	public PsqlQueryComponents buildQuery(IndexTemplate matchedIndexTemplate, List<String> indices, String[] types,
			RequestBodySearch requestBodySearch) {
		if(indices.isEmpty() && IndexUtils.isTypesEmpty(types)) {
			return new PsqlQueryComponents("", "", "", "LIMIT 0");
		}
		
		final StringBuilder whereClause = POOLED_STRING_BUILDER.get();
		
		if(!indices.isEmpty()) {
			whereClause.append("_index IN (");
			for (int i = 0; i < indices.size(); i++) {
				if (i > 0) {
					whereClause.append(',');
				}
				whereClause.append("'");
				whereClause.append(indices.get(i));
				whereClause.append("'");
			}
			whereClause.append(") ");
		}
		
		if (!IndexUtils.isTypesEmpty(types)) {
			if(!indices.isEmpty()) {
				whereClause.append(" AND ");
			}
			whereClause.append("(");
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
		
		if(!requestBodySearch.getQuery().isMatchAllQuery()) {
			whereClause.append(" AND (");
			whereClause.append(requestBodySearch.getQuerySqlWhereClause(indices, matchedIndexTemplate, indexFieldStatsService));
			whereClause.append(")");
		}

		final String result = whereClause.toString();
		return new PsqlQueryComponents(IndexUtils.DATA_TABLE, result, "", requestBodySearch.getQuerySqlOrderClause());
	}
}
