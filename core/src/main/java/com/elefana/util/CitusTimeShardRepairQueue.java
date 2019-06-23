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
package com.elefana.util;

import com.elefana.indices.psql.QueuedIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.TaskScheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CitusTimeShardRepairQueue extends HashPsqlBackedQueue<CitusTableTimestampSample> {
	private static final Logger LOGGER = LoggerFactory.getLogger(CitusTimeShardRepairQueue.class);

	private static final long IO_INTERVAL = 60000L;
	private static final int MAX_CAPACITY = 100;

	public CitusTimeShardRepairQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler) throws SQLException {
		super(jdbcTemplate, taskScheduler, IO_INTERVAL, MAX_CAPACITY);
	}

	@Override
	protected void fetchFromDatabaseUnique(JdbcTemplate jdbcTemplate, List<CitusTableTimestampSample> results, int from, int limit) throws SQLException {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
				"SELECT * FROM elefana_time_series_repair_queue ORDER BY _timestampSample ASC LIMIT " + limit + " OFFSET " + from);
		while(rowSet.next()) {
			results.add(new CitusTableTimestampSample(rowSet.getString("_index"),
					rowSet.getString("_tableName"),
					rowSet.getLong("_timestampSample")));
		}
	}

	@Override
	public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
		final String deleteQuery = "DELETE FROM elefana_time_series_repair_queue WHERE _index IN (" +
				"SELECT _index FROM elefana_time_series_repair_queue ORDER BY _timestampSample ASC LIMIT " + size + ")";
		jdbcTemplate.execute(deleteQuery);
	}

	@Override
	protected void appendToDatabaseUnique(JdbcTemplate jdbcTemplate, List<CitusTableTimestampSample> elements) throws SQLException {
		Connection connection = jdbcTemplate.getDataSource().getConnection();

		try {
			PreparedStatement preparedStatement = connection.prepareStatement(
					"INSERT INTO elefana_time_series_repair_queue (_index, _tableName, _timestampSample) VALUES (?, ?, ?) ON CONFLICT (_index) DO NOTHING;");

			final int batchSize = 100;
			int count = 0;
			int batched = 0;

			for(CitusTableTimestampSample queuedItem : elements) {
				preparedStatement.setString(1, queuedItem.getIndexName());
				preparedStatement.setString(2, queuedItem.getTableName());
				preparedStatement.setLong(3, queuedItem.getTimestampSample());
				preparedStatement.addBatch();

				count++;

				if(count % batchSize == 0) {
					preparedStatement.executeBatch();
					batched += count;
					count -= batchSize;
				}
			}

			if(batched < elements.size()) {
				preparedStatement.executeBatch();
			}
			preparedStatement.close();
			connection.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			connection.close();
			throw e;
		}
	}
}
