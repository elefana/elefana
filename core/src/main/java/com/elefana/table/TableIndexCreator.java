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
package com.elefana.table;

import com.elefana.api.indices.IndexGenerationMode;
import com.elefana.api.indices.IndexStorageSettings;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.NodeStatsService;
import com.elefana.util.DiskBackedQueue;
import com.elefana.util.IndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

@Service
@DependsOn({"nodeStatsService"})
public class TableIndexCreator implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableIndexCreator.class);

	private static final String TABLE_INDEX_QUEUE_ID = "table-index-queue";
	private static final String FIELD_INDEX_QUEUE_ID = "field-index-queue";

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeStatsService nodeStatsService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

	private DiskBackedQueue<TableIndexDelay> tableIndexQueue;
	private DiskBackedQueue<TableFieldIndexDelay> fieldIndexQueue;

	public void initialise() throws SQLException {
		if(!nodeStatsService.isMasterNode()) {
			//Only master node can create indices
			return;
		}
		final long interval = nodeSettingsService.getMappingInterval();

		tableIndexQueue = new DiskBackedQueue(TABLE_INDEX_QUEUE_ID,
				nodeSettingsService.getDataDirectory(), TableIndexDelay.class);
		fieldIndexQueue = new DiskBackedQueue(FIELD_INDEX_QUEUE_ID,
				nodeSettingsService.getDataDirectory(), TableFieldIndexDelay.class);
		executorService.scheduleAtFixedRate(this, 0L, Math.max(1000, interval), TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeStatsService.isMasterNode()) {
			//Only master node can create indices
			return;
		}
		executorService.shutdownNow();
		tableIndexQueue.dispose();
		fieldIndexQueue.dispose();
	}

	@Override
	public void run() {
		try {
			Connection connection = null;
			try {
				connection = runTableIndexCreation(connection);
				connection = runFieldIndexCreation(connection);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {}
			}

			tableIndexQueue.prune();
			fieldIndexQueue.prune();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private Connection runTableIndexCreation(Connection connection) throws SQLException {
		if(tableIndexQueue.isEmpty()) {
			LOGGER.info("No table index operations queued");
			return connection;
		}
		if(connection == null) {
			connection = jdbcTemplate.getDataSource().getConnection();
		}

		final TableIndexDelay tableIndexDelay = new TableIndexDelay();
		while(!tableIndexQueue.isEmpty()) {
			if(tableIndexQueue.peek(tableIndexDelay)) {
				if(tableIndexDelay.getIndexTimestamp() > System.currentTimeMillis()) {
					LOGGER.info("Too early to create index for " + tableIndexDelay.getTableName() + ". Remaining time: " + TimeUnit.MILLISECONDS.toMinutes(tableIndexDelay.getIndexTimestamp() - System.currentTimeMillis()) + " minutes");
					return connection;
				} else {
					LOGGER.info("Executing table index creation for " + tableIndexDelay.getTableName());
				}
			} else {
				return connection;
			}
			try {
				internalCreatePsqlTableIndices(connection, tableIndexDelay.getTableName(), tableIndexDelay.getMode(),
						tableIndexDelay.isGinEnabled(), tableIndexDelay.isBrinEnabled());
				tableIndexQueue.poll(tableIndexDelay);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				tableIndexQueue.offer(tableIndexDelay);
			}
		}
		return connection;
	}

	private Connection runFieldIndexCreation(Connection connection) throws SQLException {
		if(fieldIndexQueue.isEmpty()) {
			LOGGER.info("No table field index operations queued");
			return connection;
		}
		if(connection == null) {
			connection = jdbcTemplate.getDataSource().getConnection();
		}

		final TableFieldIndexDelay fieldIndexDelay = new TableFieldIndexDelay();
		while(!fieldIndexQueue.isEmpty()) {
			if(fieldIndexQueue.peek(fieldIndexDelay)) {
				if(fieldIndexDelay.getIndexTimestamp() > System.currentTimeMillis()) {
					LOGGER.info("Too early to create field index for " + fieldIndexDelay.getTableName() + "->" + fieldIndexDelay.getFieldName() + ". Remaining time: " + TimeUnit.MILLISECONDS.toMinutes(fieldIndexDelay.getIndexTimestamp() - System.currentTimeMillis()) + " minutes");
					return connection;
				} else {
					LOGGER.info("Executing field index creation for " + fieldIndexDelay.getTableName() + "->" + fieldIndexDelay.getFieldName());
				}
			} else {
				return connection;
			}
			try {
				internalCreatePsqlFieldIndex(connection, fieldIndexDelay.getTableName(),
						fieldIndexDelay.getFieldName(), fieldIndexDelay.getMode(),
						fieldIndexDelay.isGinEnabled(), fieldIndexDelay.isBrinEnabled(), fieldIndexDelay.isHashEnabled());
				fieldIndexQueue.poll(fieldIndexDelay);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		return connection;
	}

	public void createPsqlTableIndices(Connection connection, String tableName, IndexStorageSettings settings) throws SQLException {
		if(!settings.isGinEnabled() && !settings.isHashEnabled() && !settings.isBrinEnabled()) {
			return;
		}

		if(settings.getIndexGenerationSettings().getIndexDelaySeconds() <= 0) {
			internalCreatePsqlTableIndices(connection, tableName, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled());
		} else {
			LOGGER.info("Defer " + tableName + " index creation by " + settings.getIndexGenerationSettings().getIndexDelaySeconds() +
					" seconds (MODE:" + settings.getIndexGenerationSettings().getMode() + ", GIN:" + settings.isGinEnabled() +
					", BRIN:" + settings.isBrinEnabled() + ", HASH:" + settings.isHashEnabled() + ")");
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(settings.getIndexGenerationSettings().getIndexDelaySeconds());
			tableIndexQueue.offer(new TableIndexDelay(tableName, indexTimestamp, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled(), settings.isHashEnabled()));
		}
	}

	public void createPsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexStorageSettings settings) throws SQLException {
		if(!settings.isGinEnabled() && !settings.isHashEnabled() && !settings.isBrinEnabled()) {
			return;
		}

		if(settings.getIndexGenerationSettings().getIndexDelaySeconds() <= 0) {
			internalCreatePsqlFieldIndex(connection, tableName, fieldName, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled(), settings.isHashEnabled());
		} else {
			LOGGER.info("Defer " + tableName + "->" + fieldName + " field index creation by " + settings.getIndexGenerationSettings().getIndexDelaySeconds() +
					" seconds (MODE:" + settings.getIndexGenerationSettings().getMode() + ", GIN:" + settings.isGinEnabled() +
					", BRIN:" + settings.isBrinEnabled() + ", HASH:" + settings.isHashEnabled() + ")");
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(settings.getIndexGenerationSettings().getIndexDelaySeconds());
			fieldIndexQueue.offer(new TableFieldIndexDelay(tableName, fieldName, indexTimestamp, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled(), settings.isHashEnabled()));
		}
	}

	private void internalCreatePsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexGenerationMode mode,
	                                          boolean ginEnabled, boolean brinEnabled, boolean hashEnabled) throws SQLException {
		switch(mode) {
		case ALL:
			return;
		case PRESET:
		case DYNAMIC:
			if(brinEnabled) {
				final String btreeIndexName = getPsqlIndexName(IndexUtils.BTREE_INDEX_PREFIX, tableName, fieldName);
				final String query = "CREATE INDEX IF NOT EXISTS " + btreeIndexName + " ON " + tableName + " USING BTREE ((_source->>'" + fieldName + "'));";
				LOGGER.info(query);
				PreparedStatement preparedStatement = connection.prepareStatement(query);
				preparedStatement.execute();
				preparedStatement.close();
			}
			if(hashEnabled) {
				final String hashIndexName = getPsqlIndexName(IndexUtils.HASH_INDEX_PREFIX, tableName, fieldName);
				final String query = "CREATE INDEX IF NOT EXISTS " + hashIndexName + " ON " + tableName + " USING HASH ((_source->>'" + fieldName + "'));";
				LOGGER.info(query);
				PreparedStatement preparedStatement = connection.prepareStatement(query);
				preparedStatement.execute();
				preparedStatement.close();
			}
			if(ginEnabled) {
				final String ginIndexName = getPsqlIndexName(IndexUtils.GIN_INDEX_PREFIX, tableName, fieldName);
				final String query = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName + " USING GIN ((_source->>'" + fieldName + "'));";
				LOGGER.info(query);
				PreparedStatement preparedStatement = connection.prepareStatement(query);
				preparedStatement.execute();
				preparedStatement.close();
			}
			break;
		}
	}

	private void internalCreatePsqlTableIndices(Connection connection, String tableName,
												IndexGenerationMode indexGenerationMode,
												boolean ginEnabled, boolean brinEnabled) throws SQLException {
		final String brinIndexName = IndexUtils.BRIN_INDEX_PREFIX + tableName;
		final String ginIndexName = IndexUtils.GIN_INDEX_PREFIX + tableName;

		PreparedStatement preparedStatement;

		switch(indexGenerationMode) {
		case ALL:
			if(ginEnabled) {
				final String createGinIndexQuery = "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + ginIndexName + " ON " + tableName
						+ " USING GIN (_source jsonb_ops)";
				LOGGER.info(createGinIndexQuery);
				preparedStatement = connection.prepareStatement(createGinIndexQuery);
				preparedStatement.execute();
				preparedStatement.close();
			}
			if(brinEnabled) {
				final String createTimestampIndexQuery = "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + brinIndexName + " ON "
						+ tableName + " USING BRIN (_timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d) WITH (pages_per_range = " + nodeSettingsService.getBrinPagesPerRange() + ")";
				LOGGER.info(createTimestampIndexQuery);
				preparedStatement = connection.prepareStatement(createTimestampIndexQuery);
				preparedStatement.execute();
				preparedStatement.close();
			}
			break;
		case DYNAMIC:
			break;
		case PRESET:
			break;
		}
	}

	public static String getPsqlIndexName(String prefix, String tableName, String fieldName) {
		return prefix + tableName.replace("_m_", "__").replace("_f_", "__") + "_" + getFieldNameHash(fieldName);
	}

	public static String getFieldNameHash(String fieldName) {
		CRC32 crc = new CRC32();
		crc.update(fieldName.getBytes());
		return Long.toHexString(crc.getValue());
	}


}
