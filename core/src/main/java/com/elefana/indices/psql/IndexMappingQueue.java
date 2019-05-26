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
package com.elefana.indices.psql;

import com.elefana.util.HashPsqlBackedQueue;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.TaskScheduler;

import java.sql.SQLException;
import java.util.List;

public class IndexMappingQueue extends HashPsqlBackedQueue<QueuedIndex> {
	private static final int MAX_CAPACITY = 60 * 60;

	public IndexMappingQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler,
	                            long ioIntervalMillis) throws SQLException {
		super(jdbcTemplate, taskScheduler, ioIntervalMillis, MAX_CAPACITY);
	}

	@Override
	protected void fetchFromDatabaseUnique(JdbcTemplate jdbcTemplate, List<QueuedIndex> results, int from, int limit) throws SQLException {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_index_field_mapping_queue WHERE _timestamp <= " +
				System.currentTimeMillis() + " ORDER BY _timestamp ASC LIMIT " + limit + " OFFSET " + from);
		while(rowSet.next()) {
			results.add(new QueuedIndex(rowSet.getString("_index"),
					rowSet.getLong("_timestamp")));
		}
	}

	@Override
	public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
		final String deleteQuery = "DELETE FROM elefana_index_field_mapping_queue WHERE _index IN (" +
				"SELECT _index FROM elefana_index_field_mapping_queue ORDER BY _timestamp ASC LIMIT " + size + ")";
		jdbcTemplate.execute(deleteQuery);
	}

	@Override
	public void appendToDatabase(JdbcTemplate jdbcTemplate, List<QueuedIndex> elements) throws SQLException {
		for(QueuedIndex queuedIndex : elements) {
			jdbcTemplate.execute("INSERT INTO elefana_index_field_mapping_queue (_index, _timestamp) VALUES ('" +
					queuedIndex.getIndex() + "', " + queuedIndex.getTimestamp() + ") ON CONFLICT (_index) DO NOTHING");
		}
	}
}
