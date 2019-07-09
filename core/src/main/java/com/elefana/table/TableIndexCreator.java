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
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

@Service
@DependsOn({"nodeInfoService"})
public class TableIndexCreator implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableIndexCreator.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeInfoService nodeInfoService;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private TaskScheduler taskScheduler;

	private Queue<TableIndexDelay> tableIndexQueue;
	private Queue<TableFieldIndexDelay> fieldIndexQueue;

	@PostConstruct
	public void postConstruct() throws SQLException {
		if(!nodeInfoService.isMasterNode()) {
			//Only master node can create indices
			return;
		}
		final long interval = Math.min(nodeSettingsService.getFieldStatsInterval(), nodeSettingsService.getMappingInterval());

		tableIndexQueue = new TableIndexQueue(jdbcTemplate, taskScheduler, interval);
		fieldIndexQueue = new TableFieldIndexQueue(jdbcTemplate, taskScheduler, interval);
		taskScheduler.scheduleAtFixedRate(this, Math.max(1000, interval));
	}

	@Override
	public void run() {
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
	}

	private Connection runTableIndexCreation(Connection connection) throws SQLException {
		if(connection == null) {
			connection = jdbcTemplate.getDataSource().getConnection();
		}

		while(!tableIndexQueue.isEmpty()) {
			final TableIndexDelay tableIndexDelay = tableIndexQueue.poll();
			internalCreatePsqlTableIndices(connection, tableIndexDelay.getTableName(), tableIndexDelay.getMode(),
					tableIndexDelay.isGinEnabled(), tableIndexDelay.isBrinEnabled());
		}
		return connection;
	}

	private Connection runFieldIndexCreation(Connection connection) throws SQLException {
		if(connection == null) {
			connection = jdbcTemplate.getDataSource().getConnection();
		}

		while(!fieldIndexQueue.isEmpty()) {
			final TableFieldIndexDelay fieldIndexDelay = fieldIndexQueue.poll();
			internalCreatePsqlFieldIndex(connection, fieldIndexDelay.getTableName(),
					fieldIndexDelay.getFieldName(), fieldIndexDelay.getMode());
		}
		return connection;
	}

	public void createPsqlTableIndices(Connection connection, String tableName, IndexStorageSettings settings) throws SQLException {
		if(!settings.isBrinEnabled() && !settings.isGinEnabled()) {
			return;
		}

		if(settings.getIndexGenerationSettings().getIndexDelaySeconds() <= 0) {
			internalCreatePsqlTableIndices(connection, tableName, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled());
		} else {
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(settings.getIndexGenerationSettings().getIndexDelaySeconds());
			tableIndexQueue.offer(new TableIndexDelay(tableName, indexTimestamp, settings.getIndexGenerationSettings().getMode(),
					settings.isGinEnabled(), settings.isBrinEnabled()));
		}
	}

	public void createPsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexStorageSettings settings) throws SQLException {
		if(!settings.isGinEnabled()) {
			return;
		}

		if(settings.getIndexGenerationSettings().getIndexDelaySeconds() <= 0) {
			internalCreatePsqlFieldIndex(connection, tableName, fieldName, settings.getIndexGenerationSettings().getMode());
		} else {
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(settings.getIndexGenerationSettings().getIndexDelaySeconds());
			fieldIndexQueue.offer(new TableFieldIndexDelay(tableName, fieldName, indexTimestamp, settings.getIndexGenerationSettings().getMode()));
		}
	}

	private void internalCreatePsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexGenerationMode indexGenerationMode) throws SQLException {
		switch(indexGenerationMode) {
		case ALL:
			return;
		case PRESET:
		case DYNAMIC:
			final String btreeIndexName = IndexUtils.BTREE_INDEX_PREFIX + tableName + "_" + fieldName;
			final String createGinFieldIndexQuery = "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + btreeIndexName + " ON " + tableName + " USING BTREE ((_source->>'" + fieldName + "'));";
			LOGGER.info(createGinFieldIndexQuery);
			PreparedStatement preparedStatement = connection.prepareStatement(createGinFieldIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();
			break;
		}
	}

	private void internalCreatePsqlTableIndices(Connection connection, String tableName, IndexGenerationMode indexGenerationMode, boolean ginEnabled, boolean brinEnabled) throws SQLException {
		final String brinIndexName = IndexUtils.BRIN_INDEX_PREFIX + tableName;
		final String ginIndexName = IndexUtils.GIN_INDEX_PREFIX + tableName;

		PreparedStatement preparedStatement;

		if(indexGenerationMode.equals(IndexGenerationMode.ALL) && ginEnabled) {
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
	}
}
