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
import com.elefana.api.indices.IndexGenerationSettings;
import com.elefana.api.indices.IndexStorageSettings;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.document.psql.PsqlDocumentService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.NodeStatsService;
import com.elefana.util.DiskBackedQueue;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import org.joda.time.DateTime;
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
	private NodeSettingsService nodeSettingsService;

	private ScheduledExecutorService executorService = null;
	private DiskBackedQueue<TableFieldIndexDelay> fieldIndexQueue;

	private IndexCreatedListener listener;

	public void initialise() throws SQLException {
		if(!nodeSettingsService.isMasterNode()) {
			//Only master node can create indices
			return;
		}
		executorService = Executors.newSingleThreadScheduledExecutor(
				new NamedThreadFactory("tableIndexCreator", ThreadPriorities.PSQL_INDEX_CREATOR));
		final long interval = nodeSettingsService.getMappingInterval();

		fieldIndexQueue = new DiskBackedQueue(FIELD_INDEX_QUEUE_ID,
				nodeSettingsService.getDataDirectory(), TableFieldIndexDelay.class);
		executorService.scheduleAtFixedRate(this, 0L, Math.max(1000, interval), TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeSettingsService.isMasterNode()) {
			//Only master node can create indices
			return;
		}
		executorService.shutdownNow();
		fieldIndexQueue.dispose();
	}

	@Override
	public void run() {
		final DateTime dateTime = new DateTime();
		if(dateTime.getHourOfDay() < nodeSettingsService.getIndexTimeBoxMinHour()) {
			return;
		}
		if(dateTime.getHourOfDay() > nodeSettingsService.getIndexTimeBoxMaxHour()) {
			return;
		}

		Connection connection = null;
		try {
			try {
				connection = runFieldIndexCreation(connection);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
			fieldIndexQueue.prune();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {}
			}
		}
	}

	private Connection runFieldIndexCreation(Connection connection) throws SQLException {
		final TableFieldIndexDelay fieldIndexDelay = new TableFieldIndexDelay();
		while(fieldIndexQueue.peek(fieldIndexDelay)) {
			final DateTime dateTime = new DateTime();
			if(dateTime.getHourOfDay() < nodeSettingsService.getIndexTimeBoxMinHour()) {
				return connection;
			}
			if(dateTime.getHourOfDay() > nodeSettingsService.getIndexTimeBoxMaxHour()) {
				return connection;
			}
			if(fieldIndexDelay.getIndexTimestamp() > System.currentTimeMillis()) {
				LOGGER.info("Too early to create field index for " + fieldIndexDelay.getTableName() + "->" + fieldIndexDelay.getFieldName() + ". Remaining time: " + TimeUnit.MILLISECONDS.toMinutes(fieldIndexDelay.getIndexTimestamp() - System.currentTimeMillis()) + " minutes");
				return connection;
			} else {
				LOGGER.info("Executing field index creation for " + fieldIndexDelay.getTableName() + "->" + fieldIndexDelay.getFieldName());
			}
			if(connection == null) {
				connection = jdbcTemplate.getDataSource().getConnection();
			}
			PsqlDocumentService.DELETE_CREATE_INDEX_LOCK.lock();
			try {
				internalCreatePsqlFieldIndex(connection, fieldIndexDelay.getTableName(),
						fieldIndexDelay.getFieldName(), fieldIndexDelay.getMode(),
						fieldIndexDelay.isGinEnabled(), fieldIndexDelay.isBrinEnabled(), fieldIndexDelay.isHashEnabled());
				fieldIndexQueue.poll(fieldIndexDelay);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			} finally {
				PsqlDocumentService.DELETE_CREATE_INDEX_LOCK.unlock();
			}
		}
		return connection;
	}

	public void createPsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexStorageSettings settings) throws SQLException {
		createPsqlFieldIndex(connection, tableName, fieldName, settings, false);
	}

	public void createPsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexStorageSettings settings, boolean now) throws SQLException {
		if(settings.getIndexGenerationSettings().getMode().equals(IndexGenerationMode.NONE)) {
			return;
		}

		if(settings.getIndexGenerationSettings().getIndexDelaySeconds() <= 0 || now) {
			internalCreatePsqlFieldIndex(connection, tableName, fieldName, settings.getIndexGenerationSettings().getMode(),
					settings.getIndexGenerationSettings().getPresetGinIndexFields().contains(fieldName),
					settings.getIndexGenerationSettings().getPresetBrinIndexFields().contains(fieldName),
					settings.getIndexGenerationSettings().getPresetHashIndexFields().contains(fieldName));
		} else {
			LOGGER.info("Defer " + tableName + "->" + fieldName + " field index creation by " + settings.getIndexGenerationSettings().getIndexDelaySeconds() +
					" seconds (MODE:" + settings.getIndexGenerationSettings().getMode() +
					", GIN:" + settings.getIndexGenerationSettings().getPresetGinIndexFields().contains(fieldName) +
					", BRIN:" + settings.getIndexGenerationSettings().getPresetBrinIndexFields().contains(fieldName) +
					", HASH:" + settings.getIndexGenerationSettings().getPresetHashIndexFields().contains(fieldName) + ") (" + hashCode() + ")");
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(settings.getIndexGenerationSettings().getIndexDelaySeconds());
			if(!fieldIndexQueue.offer(new TableFieldIndexDelay(tableName, fieldName, indexTimestamp, settings.getIndexGenerationSettings().getMode(),
					settings.getIndexGenerationSettings().getPresetGinIndexFields().contains(fieldName),
					settings.getIndexGenerationSettings().getPresetBrinIndexFields().contains(fieldName),
					settings.getIndexGenerationSettings().getPresetHashIndexFields().contains(fieldName) ))) {
				LOGGER.error("Could not offer to field index queue");
			}
		}
	}

	private void internalCreatePsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexGenerationMode mode,
	                                          boolean ginEnabled, boolean brinEnabled, boolean hashEnabled) throws SQLException {
		if(brinEnabled) {
			final String btreeIndexName = getPsqlIndexName(IndexUtils.BTREE_INDEX_PREFIX, tableName, fieldName);
			final String query = "CREATE INDEX IF NOT EXISTS " + btreeIndexName + " ON " + tableName + " USING BTREE ((_source->>'" + fieldName + "'));";
			LOGGER.info(query);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			try {
				preparedStatement.execute();
				preparedStatement.close();
			} catch (SQLException e) {
				abortPreparedStatement(preparedStatement);
				throw e;
			}

			if(listener != null) {
				listener.onCreated();
			}
		}
		if(hashEnabled) {
			final String hashIndexName = getPsqlIndexName(IndexUtils.HASH_INDEX_PREFIX, tableName, fieldName);
			final String query = "CREATE INDEX IF NOT EXISTS " + hashIndexName + " ON " + tableName + " USING HASH ((_source->>'" + fieldName + "'));";
			LOGGER.info(query);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			try {
				preparedStatement.execute();
				preparedStatement.close();
			} catch (SQLException e) {
				abortPreparedStatement(preparedStatement);
				throw e;
			}

			if(listener != null) {
				listener.onCreated();
			}
		}
		if(ginEnabled) {
			final String ginIndexName = getPsqlIndexName(IndexUtils.GIN_INDEX_PREFIX, tableName, fieldName);
			final String query = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName + " USING GIN ((_source->>'" + fieldName + "') gin_trgm_ops);";
			LOGGER.info(query);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			try {
				preparedStatement.execute();
				preparedStatement.close();
			} catch (SQLException e) {
				abortPreparedStatement(preparedStatement);
				throw e;
			}

			if(listener != null) {
				listener.onCreated();
			}
		}
	}

	public void deletePsqlIndices(String index, String tableName, IndexTemplate indexTemplate) {
		if(indexTemplate.getStorage() == null) {
			return;
		}
		if(indexTemplate.getStorage().getIndexGenerationSettings() == null) {
			return;
		}

		Connection connection = null;

		try {
			final IndexGenerationSettings settings = indexTemplate.getStorage().getIndexGenerationSettings();
			if(settings.getPresetBrinIndexFields() != null) {
				for(String fieldName : settings.getPresetBrinIndexFields()) {
					if(connection == null) {
						connection = jdbcTemplate.getDataSource().getConnection();
					}
					final String btreeIndexName = getPsqlIndexName(IndexUtils.BTREE_INDEX_PREFIX, tableName, fieldName);
					final String query = "DROP INDEX IF EXISTS " + btreeIndexName;
					PreparedStatement preparedStatement = connection.prepareStatement(query);
					try {
						preparedStatement.execute();
						preparedStatement.close();
					} catch (SQLException e) {
						abortPreparedStatement(preparedStatement);
						throw e;
					}
				}
			}
			if(settings.getPresetGinIndexFields() != null) {
				for(String fieldName : settings.getPresetGinIndexFields()) {
					if(connection == null) {
						connection = jdbcTemplate.getDataSource().getConnection();
					}
					final String ginIndexName = getPsqlIndexName(IndexUtils.GIN_INDEX_PREFIX, tableName, fieldName);
					final String query = "DROP INDEX IF EXISTS " + ginIndexName;
					PreparedStatement preparedStatement = connection.prepareStatement(query);
					try {
						preparedStatement.execute();
						preparedStatement.close();
					} catch (SQLException e) {
						abortPreparedStatement(preparedStatement);
						throw e;
					}
				}
			}
			if(settings.getPresetHashIndexFields() != null) {
				for(String fieldName : settings.getPresetHashIndexFields()) {
					if(connection == null) {
						connection = jdbcTemplate.getDataSource().getConnection();
					}
					final String hashIndexName = getPsqlIndexName(IndexUtils.HASH_INDEX_PREFIX, tableName, fieldName);
					final String query = "DROP INDEX IF EXISTS " + hashIndexName;
					PreparedStatement preparedStatement = connection.prepareStatement(query);
					try {
						preparedStatement.execute();
						preparedStatement.close();
					} catch (SQLException e) {
						abortPreparedStatement(preparedStatement);
						throw e;
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {}
			}
		}
	}

	private void abortPreparedStatement(PreparedStatement preparedStatement) {
		try {
			preparedStatement.cancel();
		} catch (Exception e) {}
		try {
			preparedStatement.close();
		} catch (Exception e) {}
	}

	public static String getPsqlIndexName(String prefix, String tableName, String fieldName) {
		return prefix + tableName.replace("_m_", "__").replace("_f_", "__") + "_" + getFieldNameHash(fieldName);
	}

	public static String getFieldNameHash(String fieldName) {
		CRC32 crc = new CRC32();
		crc.update(fieldName.getBytes());
		return Long.toHexString(crc.getValue());
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setNodeSettingsService(NodeSettingsService nodeSettingsService) {
		this.nodeSettingsService = nodeSettingsService;
	}

	public void setListener(IndexCreatedListener listener) {
		this.listener = listener;
	}

	public interface IndexCreatedListener {

		public void onCreated();
	}
}
