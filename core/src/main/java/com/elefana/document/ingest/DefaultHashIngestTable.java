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
package com.elefana.document.ingest;

import com.elefana.api.exception.ElefanaException;
import com.elefana.util.IndexUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultHashIngestTable implements HashIngestTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultHashIngestTable.class);

	private final String index;
	private final ReentrantLock[] locks;
	private final String [] tableNames;
	private final ThreadLocal<Integer> readIndex = new ThreadLocal<Integer>() {
		@Override
		protected Integer initialValue() {
			return (int) Thread.currentThread().getId() % tableNames.length;
		}
	};
	private final ThreadLocal<Integer> writeIndex = new ThreadLocal<Integer>() {
		@Override
		protected Integer initialValue() {
			return (int) Thread.currentThread().getId() % tableNames.length;
		}
	};
	private final boolean [] dataMarker;

	public DefaultHashIngestTable(JdbcTemplate jdbcTemplate, String [] tablespaces,
	                              String index, int capacity, List<String> existingTableNames) throws SQLException {
		super();
		this.index = index;

		locks = new ReentrantLock[capacity];
		tableNames = new String[capacity];
		dataMarker = new boolean[capacity];

		for(int i = 0; i < existingTableNames.size() && i < tableNames.length; i++) {
			tableNames[i] = existingTableNames.get(i);
		}

		Connection connection = null;

		for(int i = 0; i < capacity; i++) {
			locks[i] = new ReentrantLock();
			dataMarker[i] = false;

			if(tableNames[i] == null) {
				if(connection == null) {
					connection = jdbcTemplate.getDataSource().getConnection();
				}
				tableNames[i] = createAndStoreStagingTable(connection, tablespaces.length > 0 ? tablespaces[i % tablespaces.length] : null);
			}
		}

		if(connection != null) {
			try {
				connection.close();
			} catch (Exception e) {}
		}
	}

	private String createAndStoreStagingTable(Connection connection, String tablespace) throws SQLException {
		final String tableName = getNextStagingTable(connection);
		createStageTable(connection, tableName, tablespace);
		storeStagingTable(connection, tableName);
		return tableName;
	}

	private void createStageTable(Connection connection, String tableName, String tablespace) throws SQLException {
		final StringBuilder createTableQuery = IndexUtils.POOLED_STRING_BUILDER.get();
		createTableQuery.append("CREATE TABLE IF NOT EXISTS ");
		createTableQuery.append(tableName);
		createTableQuery.append(" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, ");
		createTableQuery.append("_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb)");
		if (tablespace != null && !tablespace.isEmpty()) {
			createTableQuery.append(" TABLESPACE ");
			createTableQuery.append(tablespace);
		}
		createTableQuery.append(" WITH (autovacuum_enabled=false)");

		PreparedStatement createTableStatement = connection.prepareStatement(createTableQuery.toString());
		createTableStatement.execute();
		createTableStatement.close();
	}

	private void storeStagingTable(Connection connection, String tableName) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO elefana_bulk_tables (_index, _ingestTableName) VALUES (?, ?)");
		preparedStatement.setString(1, index);
		preparedStatement.setString(2, tableName);
		preparedStatement.execute();
		preparedStatement.close();
	}

	private String getNextStagingTable(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("SELECT elefana_next_bulk_ingest_table()");
		final ResultSet resultSet = preparedStatement.executeQuery();
		String tableName = null;
		if(resultSet.next()) {
			try {
				tableName = resultSet.getString("table_name").replace('-', '_');
			} catch (Exception e) {
				tableName = resultSet.getString("elefana_next_bulk_ingest_table").replace('-', '_');
			}
		}
		preparedStatement.close();

		if(tableName == null) {
			throw new RuntimeException("Could not get next staging table ID from elefana_next_staging_table()");
		}
		return tableName;
	}

	public int lockTable() throws ElefanaException {
		return lockTable(2L * locks.length);
	}

	public int lockTable(long timeout) throws ElefanaException {
		final long timestamp = System.currentTimeMillis();
		while(System.currentTimeMillis() - timestamp < timeout) {
			for(int i = 0; i < locks.length; i++) {
				int index = writeIndex.get() % locks.length;
				writeIndex.set(index + 1);
				if(locks[index].tryLock()) {
					return index;
				}
			}
		}
		throw new ElefanaException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Unable to lock ingest table for index '" + index + "'");
	}

	public int lockWrittenTable() throws ElefanaException {
		return lockWrittenTable(5L * locks.length);
	}

	public int lockWrittenTable(long timeout) throws ElefanaException {
		final long timestamp = System.currentTimeMillis();
		while(System.currentTimeMillis() - timestamp < timeout) {
			for(int i = 0; i < locks.length; i++) {
				int index = readIndex.get() % locks.length;
				readIndex.set(index + 1);
				try {
					if(locks[index].tryLock()) {
						if(!isDataMarked(index)) {
							locks[index].unlock();
							continue;
						}
						return index;
					}
					if(System.currentTimeMillis() - timestamp >= timeout) {
						return -1;
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}
		return -1;
	}

	public boolean isDataMarked(int index) {
		if(locks[index].getHoldCount() == 0) {
			throw new RuntimeException("Cannot check mark status without lock acquired");
		}
		return dataMarker[index];
	}

	public void markData(int index) {
		if(locks[index].getHoldCount() == 0) {
			return;
		}
		dataMarker[index] = true;
	}

	public void unmarkData(int index) {
		if(locks[index].getHoldCount() == 0) {
			return;
		}
		dataMarker[index] = false;
	}

	public void unlockTable(int index) {
		if(locks[index].getHoldCount() == 0) {
			return;
		}
		locks[index].unlock();
	}

	public String getIngestionTableName(int index) {
		if(locks[index].getHoldCount() == 0) {
			throw new RuntimeException("Cannot get ingestion table name without lock acquired");
		}
		return tableNames[index];
	}

	public String getIndex() {
		return index;
	}

	public int getCapacity() {
		return dataMarker.length;
	}
}