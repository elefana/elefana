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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultHashIngestTable implements HashIngestTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultHashIngestTable.class);

	private final JdbcTemplate jdbcTemplate;
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
	private final AtomicLong lastUsageTimestamp = new AtomicLong();

	public DefaultHashIngestTable(JdbcTemplate jdbcTemplate, String [] tablespaces,
	                              String index, int capacity, List<String> existingTableNames) throws SQLException {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.index = index;

		locks = new ReentrantLock[capacity];
		tableNames = new String[capacity];
		dataMarker = new boolean[capacity];

		Connection connection = null;

		for(int i = 0; i < existingTableNames.size() && i < tableNames.length; i++) {
			tableNames[i] = existingTableNames.get(i);

			if(connection == null) {
				connection = jdbcTemplate.getDataSource().getConnection();
			}
			dataMarker[i] = getExistingTableCount(connection, tableNames[i]) > 0;
		}
		lastUsageTimestamp.set(System.currentTimeMillis());

		for(int i = 0; i < capacity; i++) {
			locks[i] = new ReentrantLock();

			if(tableNames[i] == null) {
				if(connection == null) {
					connection = jdbcTemplate.getDataSource().getConnection();
				}
				tableNames[i] = createAndStoreStagingTable(connection, tablespaces.length > 0 ? tablespaces[i % tablespaces.length] : null);
				dataMarker[i] = false;
			}
		}

		if(connection != null) {
			try {
				connection.close();
			} catch (Exception e) {}
		}
	}

	private boolean tryLockAll() {
		for(int i = 0; i < locks.length; i++) {
			if(!locks[i].tryLock()) {
				for(int j = i - 1; j >= 0; j--) {
					locks[j].unlock();
				}
				return false;
			}
		}
		return true;
	}

	private void unlockAll() {
		for(int i = 0; i < locks.length; i++) {
			if(locks[i].getHoldCount() > 0) {
				locks[i].unlock();
			}
		}
	}

	@Override
	public boolean prune() {
		if(!tryLockAll()) {
			return false;
		}

		Connection connection = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			boolean atLeast1Entry = false;

			for(int i = 0; i < locks.length; i++) {
				PreparedStatement countEntriesStatement = connection.prepareStatement("SELECT COUNT(*) FROM " + tableNames[i]);
				final ResultSet resultSet = countEntriesStatement.executeQuery();
				if(resultSet.next()) {
					atLeast1Entry |= resultSet.getLong(1) > 0;
				}
				countEntriesStatement.close();
			}

			if(atLeast1Entry) {
				connection.close();
				unlockAll();
				return false;
			}

			for(int i = 0; i < locks.length; i++) {
				PreparedStatement dropTableStatement = connection.prepareStatement("DROP TABLE " + tableNames[i]);
				dropTableStatement.execute();
				dropTableStatement.close();

				PreparedStatement deleteTableStatement = connection.prepareStatement("DELETE FROM elefana_bulk_tables WHERE _ingestTableName = '" + tableNames[i] + "'");
				deleteTableStatement.execute();
				deleteTableStatement.close();
			}

			connection.close();
			return true;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);

			if(connection != null) {
				try {
					connection.close();
				} catch (Exception ex) {}
			}

			unlockAll();
			return false;
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

	private int getExistingTableCount(Connection connection, String tableName) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM " + tableName);
		final ResultSet resultSet = preparedStatement.executeQuery();
		int result = 0;
		if(resultSet.next()) {
			result = resultSet.getInt("count");
		}
		preparedStatement.close();
		return result;
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
		return lockTable(1L);
	}

	public int lockTable(long timeoutMillis) throws ElefanaException {
		final long timestamp = System.currentTimeMillis();
		while(System.currentTimeMillis() - timestamp < timeoutMillis) {
			for(int i = 0; i < locks.length; i++) {
				int index = writeIndex.get() % locks.length;
				writeIndex.set(index + 1);
				if(locks[index].tryLock()) {
					lastUsageTimestamp.set(System.currentTimeMillis());
					return index;
				}
			}
		}
		throw new ElefanaException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Unable to lock ingest table for index '" + index + "'");
	}

	public int lockWrittenTable() throws ElefanaException {
		return lockWrittenTable(100000L);
	}

	public int lockWrittenTable(long timeoutNanos) throws ElefanaException {
		final long timestamp = System.nanoTime();
		while(System.nanoTime() - timestamp < timeoutNanos) {
			for(int i = 0; i < locks.length; i++) {
				int index = readIndex.get() % locks.length;
				readIndex.set(index + 1);
				try {
					if(locks[index].tryLock()) {
						if(!isDataMarked(index)) {
							locks[index].unlock();
							continue;
						}
						lastUsageTimestamp.set(System.currentTimeMillis());
						return index;
					}
					if(System.nanoTime() - timestamp >= timeoutNanos) {
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
		lastUsageTimestamp.set(System.currentTimeMillis());
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

	@Override
	public long getLastUsageTimestamp() {
		return lastUsageTimestamp.get();
	}
}
