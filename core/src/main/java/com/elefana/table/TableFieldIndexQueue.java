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

public class TableFieldIndexQueue extends PsqlBackedQueue<TableFieldIndexDelay> {
	private static final int MAX_CAPACITY = 500;

	public TableFieldIndexQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler, long ioIntervalMillis) throws SQLException {
		super(jdbcTemplate, taskScheduler, ioIntervalMillis, MAX_CAPACITY);
	}

	@Override
	public void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<TableFieldIndexDelay> results, int from, int limit) throws SQLException {
		final String fetchQueueQuery = "SELECT * FROM elefana_delayed_field_index_queue WHERE _timestamp <= " +
				System.currentTimeMillis() + " ORDER BY _timestamp ASC LIMIT " + limit + " OFFSET " + from;
		final SqlRowSet resultSet = jdbcTemplate.queryForRowSet(fetchQueueQuery);
		while(resultSet.next()) {
			final String tableName = resultSet.getString("_tableName");
			final String fieldName = resultSet.getString("_fieldName");
			final long timestamp = resultSet.getLong("_timestamp");
			final IndexGenerationMode indexGenerationMode = IndexGenerationMode.valueOf(resultSet.getString("_generationMode"));
			results.add(new TableFieldIndexDelay(tableName, fieldName, timestamp, indexGenerationMode));
		}
	}

	@Override
	public int getDatabaseQueueSize(JdbcTemplate jdbcTemplate) throws SQLException {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT COUNT(*) FROM elefana_delayed_field_index_queue");
		if(rowSet.next()) {
			return rowSet.getInt(1);
		}
		return 0;
	}

	@Override
	public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
		final String deleteQuery = "DELETE FROM elefana_delayed_table_index_queue WHERE _tableName IN (" +
				"SELECT _tableName FROM elefana_delayed_table_index_queue ORDER BY _timestamp ASC LIMIT " + size + ") AND _fieldName IN (" +
				"SELECT _fieldName FROM elefana_delayed_table_index_queue ORDER BY _timestamp ASC LIMIT " + size + ")";
		jdbcTemplate.execute(deleteQuery);
	}

	@Override
	public void appendToDatabase(JdbcTemplate jdbcTemplate, List<TableFieldIndexDelay> elements) throws SQLException {
		for(TableFieldIndexDelay fieldIndexDelay : elements) {
			jdbcTemplate.execute("INSERT INTO elefana_delayed_field_index_queue (_tableName, _fieldName, _timestamp, _generationMode) VALUES ('" +
					fieldIndexDelay.getTableName() + "', '" + fieldIndexDelay.getFieldName() + "', " + fieldIndexDelay.getIndexTimestamp() + ", '" + fieldIndexDelay.getMode().name() + "')");
		}
	}
}
