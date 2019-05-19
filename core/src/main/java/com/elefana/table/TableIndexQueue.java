/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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
package com.elefana.table;

import com.elefana.api.indices.IndexGenerationMode;
import com.elefana.util.PsqlBackedQueue;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.TaskScheduler;

import java.sql.SQLException;
import java.util.List;

public class TableIndexQueue extends PsqlBackedQueue<TableIndexDelay> {
	private static final int MAX_CAPACITY = 100;

	public TableIndexQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler, long ioIntervalMillis) throws SQLException {
		super(jdbcTemplate, taskScheduler, ioIntervalMillis, MAX_CAPACITY);
	}

	@Override
	public void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<TableIndexDelay> results, int from, int limit) throws SQLException {
		final String fetchQueueQuery = "SELECT * FROM elefana_delayed_table_index_queue WHERE _timestamp <= " +
				System.currentTimeMillis() + " ORDER BY _timestamp ASC LIMIT " + limit + " OFFSET " + from;
		final SqlRowSet resultSet = jdbcTemplate.queryForRowSet(fetchQueueQuery);
		while(resultSet.next()) {
			final String tableName = resultSet.getString("_tableName");
			final long timestamp = resultSet.getLong("_timestamp");
			final IndexGenerationMode indexGenerationMode = IndexGenerationMode.valueOf(resultSet.getString("_generationMode"));
			results.add(new TableIndexDelay(tableName, timestamp, indexGenerationMode));
		}
	}

	@Override
	public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
		final String deleteQuery = "DELETE FROM elefana_delayed_table_index_queue WHERE _tableName IN (" +
				"SELECT _tableName FROM elefana_delayed_table_index_queue ORDER BY _timestamp ASC LIMIT " + size + ")";
		jdbcTemplate.execute(deleteQuery);
	}

	@Override
	public void appendToDatabase(JdbcTemplate jdbcTemplate, List<TableIndexDelay> elements) throws SQLException {
		for(TableIndexDelay indexDelay : elements) {
			jdbcTemplate.execute("INSERT INTO elefana_delayed_table_index_queue (_tableName, _timestamp, _generationMode) VALUES ('" +
					indexDelay.getTableName() + "', " + indexDelay.getIndexTimestamp() + ", '" + indexDelay.getMode().name() + "')");
		}
	}
}
