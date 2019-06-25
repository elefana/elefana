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

import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.util.IndexUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CitusSearchQueryBuilder implements SearchQueryBuilder {
	private final JdbcTemplate jdbcTemplate;
	private final IndexUtils indexUtils;

	public CitusSearchQueryBuilder(JdbcTemplate jdbcTemplate, IndexUtils indexUtils) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.indexUtils = indexUtils;
	}

	@Override
	public PsqlQueryComponents buildQuery(IndexTemplate matchedIndexTemplate, List<String> indices, String[] types, RequestBodySearch requestBodySearch) throws ShardFailedException {
		if(indices.isEmpty()) {
			final String emptyDataTableName = SEARCH_TABLE_PREFIX + requestBodySearch.hashCode();

			//TODO: Creating table is ineffecient - is there a better way to return an empty set with correct column names?
			Connection connection = null;
			try {
				connection = jdbcTemplate.getDataSource().getConnection();

				final StringBuilder emptyTableQuery = new StringBuilder();
				emptyTableQuery.append("CREATE TABLE ");
				emptyTableQuery.append(emptyDataTableName);
				emptyTableQuery.append(
						" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, "
								+ "_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb)");

				PreparedStatement preparedStatement = connection
						.prepareStatement(emptyTableQuery.toString());
				preparedStatement.execute();
				preparedStatement.close();

				preparedStatement = connection
						.prepareStatement("SELECT create_distributed_table('" + emptyDataTableName + "', '_id');");
				preparedStatement.execute();
				preparedStatement.close();

				connection.close();
			} catch (Exception e) {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
				e.printStackTrace();
				throw new ShardFailedException(e);
			}

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
