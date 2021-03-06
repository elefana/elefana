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
import com.elefana.api.indices.IndexTimeBucket;
import com.elefana.api.util.ThreadLocalInteger;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultTimeIngestTable implements TimeIngestTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTimeIngestTable.class);

	private final JdbcTemplate jdbcTemplate;
	private final String index;
	private final int capacity;
	private final ReentrantLock[] locks;
	private final String [] tableNames;
	private final int [] shardIds;
	private final AtomicInteger[] dataMarker;
	private final AtomicLong lastUsageTimestamp = new AtomicLong();
	private final AtomicBoolean pruned = new AtomicBoolean();

	private final ThreadLocalInteger readIndex;

	public DefaultTimeIngestTable(JdbcTemplate jdbcTemplate, String [] tablespaces,
	                              String index, IndexTimeBucket timeBucket, int bulkParallelisation, List<String> existingTableNames) throws SQLException {
		super();
		this.index = index;
		this.capacity = (timeBucket.getIngestTableCapacity() * bulkParallelisation) + (bulkParallelisation + 1);
		this.jdbcTemplate = jdbcTemplate;

		locks = new ReentrantLock[capacity];
		tableNames = new String[capacity];
		shardIds = new int[capacity];
		dataMarker = new AtomicInteger[capacity];

		readIndex = new ThreadLocalInteger(() -> {
			return (int) Thread.currentThread().getId() % tableNames.length;
		});

		for(int i = 0; i < shardIds.length; i++) {
			shardIds[i] = -1;
		}

		if(existingTableNames.isEmpty()) {
			lastUsageTimestamp.set(System.currentTimeMillis());
		}

		Connection connection = null;

		try {
			for(int i = 0; i < existingTableNames.size() && i < tableNames.length; i++) {
				tableNames[i] = existingTableNames.get(i);
			}

			for(int i = 0; i < capacity; i++) {
				locks[i] = new ReentrantLock();

				if(connection == null) {
					connection = jdbcTemplate.getDataSource().getConnection();
				}

				dataMarker[i] = new AtomicInteger(0);
				if(tableNames[i] == null) {
					tableNames[i] = createAndStoreStagingTable(connection, tablespaces.length > 0 ? tablespaces[i % tablespaces.length] : null);
				} else {
					restoreExistingTable(connection, timeBucket, i, tableNames[i]);
				}
			}

			closeConnection(connection);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
			closeConnection(connection);
			throw e;
		}
	}

	private void closeConnection(Connection connection) {
		if(connection == null) {
			return;
		}
		try {
			connection.close();
		} catch (Exception ex) {}
	}

	@Override
	public long getLastUsageTimestamp() {
		return lastUsageTimestamp.get();
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
		for(int i = 0; i < locks.length; i++) {
			if(isDataMarked(i)) {
				LOGGER.info(index + " can't prune - data marked");
				return false;
			}
		}
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
				lastUsageTimestamp.set(System.currentTimeMillis());
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
			pruned.set(true);
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

	private void restoreExistingTable(Connection connection, IndexTimeBucket timeBucket, int arrayIndex, String existingTableName) throws SQLException {
		final PreparedStatement createTableStatement = connection.prepareStatement("SELECT _timestamp FROM " + existingTableName + " LIMIT 1");
		final ResultSet createTableResultSet = createTableStatement.executeQuery();
		if(createTableResultSet.next()) {
			shardIds[arrayIndex] = timeBucket.getShardOffset(createTableResultSet.getLong("_timestamp"));
			lastUsageTimestamp.set(System.currentTimeMillis());
		}
		createTableStatement.close();

		final PreparedStatement countStatement = connection.prepareStatement("SELECT COUNT(*) FROM " + existingTableName);
		final ResultSet countResultSet = createTableStatement.executeQuery();
		if(countResultSet.next()) {
			dataMarker[arrayIndex].set(countResultSet.getInt(1));
		}
		countStatement.close();
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

	@Override
	public int lockTable(int shardOffset) throws ElefanaException {
		return lockTable(shardOffset, 2L * locks.length);
	}

	@Override
	public int lockTable(int shardOffset, long timeout) throws ElefanaException {
		final long timestamp = System.currentTimeMillis();
		while(System.currentTimeMillis() - timestamp < timeout) {
			for(int i = 0; i < locks.length; i++) {
				//Sequential to encourage re-use of table offsets
				final int index = i;
				if(locks[index].tryLock()) {
					if(shardIds[index] < 0) {
						shardIds[index] = shardOffset;
					}
					if(shardIds[index] == shardOffset) {
						lastUsageTimestamp.set(System.currentTimeMillis());
						return index;
					}
					locks[index].unlock();
					continue;
				}
			}
		}
		throw new ElefanaException(HttpResponseStatus.TOO_MANY_REQUESTS, "Unable to lock ingest table for index '" + index + "'");
	}

	@Override
	public int getOffsetedIndex(int index) {
		return index;
	}

	public int lockWrittenTable(Set<String> routedTables) throws ElefanaException {
		return lockWrittenTable(routedTables, 5L * locks.length);
	}

	public int lockWrittenTable(Set<String> routedTables, long timeout) throws ElefanaException {
		if(pruned.get()) {
			return -1;
		}
		final long timestamp = System.currentTimeMillis();
		while(System.currentTimeMillis() - timestamp < timeout) {
			for(int i = 0; i < locks.length; i++) {
				int index = Math.abs(readIndex.incrementAndGet() % locks.length);
				if(pruned.get()) {
					return -1;
				}
				try {
					if(!isDataMarked(index)) {
						continue;
					}
					if(routedTables.contains(tableNames[index])) {
						continue;
					}
					if(locks[index].tryLock()) {
						if(!isDataMarked(index)) {
							locks[index].unlock();
							continue;
						}
						lastUsageTimestamp.set(System.currentTimeMillis());
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
		return dataMarker[index].get()  > 0;
	}

	@Override
	public void markData(int index, int quantity, boolean skipLockCheck) {
		if(!skipLockCheck) {
			if(locks[index].getHoldCount() == 0) {
				return;
			}
		}
		dataMarker[index].addAndGet(quantity);
	}

	@Override
	public void unmarkData(int index, boolean skipLockCheck) {
		if(!skipLockCheck) {
			if(locks[index].getHoldCount() == 0) {
				return;
			}
		}
		shardIds[index] = -1;
		dataMarker[index].set(0);
	}

	@Override
	public int getDataCount(int index) {
		return dataMarker[index].get();
	}

	public void unlockTable(int index) {
		if(locks[index].getHoldCount() == 0) {
			return;
		}
		lastUsageTimestamp.set(System.currentTimeMillis());
		locks[index].unlock();
	}

	@Override
	public String getIngestionTableName(int index, boolean skipLockCheck) {
		if(!skipLockCheck) {
			if(locks[index].getHoldCount() == 0) {
				throw new RuntimeException("Cannot get ingestion table name without lock acquired");
			}
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
