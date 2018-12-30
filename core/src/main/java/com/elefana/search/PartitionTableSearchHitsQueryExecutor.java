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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.codahale.metrics.Histogram;

public class PartitionTableSearchHitsQueryExecutor extends SearchHitsQueryExecutor {

	public PartitionTableSearchHitsQueryExecutor(JdbcTemplate jdbcTemplate, Histogram searchTime,
			Histogram searchHits) {
		super(jdbcTemplate, searchTime, searchHits);
	}

	@Override
	public SqlRowSet queryHits(PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		final StringBuilder queryBuilder = new StringBuilder();
		if (queryComponents.getFromComponent().isEmpty()) {
			return null;
		}

		queryBuilder.append("SELECT ");
		queryBuilder.append("*");
		queryBuilder.append(" FROM ");
		queryBuilder.append(queryComponents.getFromComponent());

		if (!queryComponents.getWhereComponent().isEmpty()) {
			queryBuilder.append(" WHERE ");
			queryBuilder.append(queryComponents.getWhereComponent());
		}
		if (!queryComponents.getOrderByComponent().isEmpty()) {
			queryBuilder.append(" ORDER BY ");
			queryBuilder.append(queryComponents.getOrderByComponent());
		}
		if(size > 0) {
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(size);
		}
		if(from > 0) {
			queryBuilder.append(" OFFSET ");
			queryBuilder.append(from);
		}
		return jdbcTemplate.queryForRowSet(queryBuilder.toString());
	}

	@Override
	public SqlRowSet queryHitsCount(PsqlQueryComponents queryComponents, long startTime, int from, int size) {
		final StringBuilder queryBuilder = new StringBuilder();
		if (queryComponents.getFromComponent().isEmpty()) {
			return null;
		}

		queryBuilder.append("SELECT ");
		queryBuilder.append("COUNT(_id)");
		queryBuilder.append(" FROM ");
		queryBuilder.append(queryComponents.getFromComponent());

		if (!queryComponents.getWhereComponent().isEmpty()) {
			queryBuilder.append(" WHERE ");
			queryBuilder.append(queryComponents.getWhereComponent());
		}
		return jdbcTemplate.queryForRowSet(queryBuilder.toString());
	}

}
