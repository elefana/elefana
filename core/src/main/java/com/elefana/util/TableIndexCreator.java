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
package com.elefana.util;

import com.elefana.api.indices.IndexGenerationMode;
import com.elefana.api.indices.IndexGenerationSettings;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

@Service
@DependsOn({"nodeInfoService"})
public class TableIndexCreator implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableIndexCreator.class);

	private final Queue<String> tableIndexQueue = new LinkedList<String>();
	private final Map<String, IndexGenerationMode> tableIndexModes = new HashMap<String, IndexGenerationMode>();

	private final Queue<String> tableFieldIndexQueue = new LinkedList<String>();
	private final Queue<String> fieldIndexQueue = new LinkedList<String>();
	private final Map<String, IndexGenerationMode> tableFieldIndexModes = new HashMap<String, IndexGenerationMode>();

	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeInfoService nodeInfoService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	@PostConstruct
	public void postConstruct() throws SQLException {
		if(!nodeInfoService.isMasterNode()) {
			//Only master node can create indices
			scheduledExecutorService.shutdown();
			return;
		}
		final long interval = Math.min(nodeSettingsService.getFieldStatsInterval(), nodeSettingsService.getMappingInterval());
		scheduledExecutorService.scheduleAtFixedRate(this, 30000L, Math.max(1000, interval), TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeInfoService.isMasterNode()) {
			return;
		}
		scheduledExecutorService.shutdown();
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
		final String fetchQueueQuery = "SELECT * FROM elefana_delayed_table_index_queue WHERE _timestamp <= " + System.currentTimeMillis();
		PreparedStatement preparedStatement = connection.prepareStatement(fetchQueueQuery);
		final ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next()) {
			final String tableName = resultSet.getString("_tableName");
			final IndexGenerationMode indexGenerationMode = IndexGenerationMode.valueOf(resultSet.getString("_generationMode"));
			tableIndexQueue.offer(tableName);
			tableIndexModes.put(tableName, indexGenerationMode);
		}
		preparedStatement.close();

		while(!tableIndexQueue.isEmpty()) {
			final String tableName = tableIndexQueue.poll();
			final IndexGenerationMode indexGenerationMode = tableIndexModes.remove(tableName);

			internalCreatePsqlTableIndices(connection, tableName, indexGenerationMode);
		}
		return connection;
	}

	private Connection runFieldIndexCreation(Connection connection) throws SQLException {
		if(connection == null) {
			connection = jdbcTemplate.getDataSource().getConnection();
		}
		final String fetchQueueQuery = "SELECT * FROM elefana_delayed_field_index_queue WHERE _timestamp <= " + System.currentTimeMillis();
		PreparedStatement preparedStatement = connection.prepareStatement(fetchQueueQuery);
		final ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next()) {
			final String tableName = resultSet.getString("_tableName");
			final String fieldName = resultSet.getString("_fieldName");
			final IndexGenerationMode indexGenerationMode = IndexGenerationMode.valueOf(resultSet.getString("_generationMode"));
			tableFieldIndexQueue.offer(tableName);
			fieldIndexQueue.offer(fieldName);
			tableFieldIndexModes.put(tableName, indexGenerationMode);
		}
		preparedStatement.close();

		while(!tableFieldIndexQueue.isEmpty()) {
			final String tableName = tableFieldIndexQueue.poll();
			final String fieldName = fieldIndexQueue.poll();
			final IndexGenerationMode indexGenerationMode = tableFieldIndexModes.get(tableName);

			internalCreatePsqlFieldIndex(connection, tableName, fieldName, indexGenerationMode);
		}

		tableFieldIndexModes.clear();
		return connection;
	}

	public void createPsqlTableIndices(Connection connection, String tableName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		if(indexGenerationSettings.getIndexDelaySeconds() <= 0) {
			internalCreatePsqlTableIndices(connection, tableName, indexGenerationSettings.getMode());
		} else {
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(indexGenerationSettings.getIndexDelaySeconds());
			jdbcTemplate.execute("INSERT INTO elefana_delayed_table_index_queue (_tableName, _timestamp, _generationMode) VALUES ('" +
					tableName + "', " + indexTimestamp + ", '" + indexGenerationSettings.getMode().name() + "')");
		}
	}

	public void createPsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		if(indexGenerationSettings.getIndexDelaySeconds() <= 0) {
			internalCreatePsqlFieldIndex(connection, tableName, fieldName, indexGenerationSettings.getMode());
		} else {
			final long indexTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(indexGenerationSettings.getIndexDelaySeconds());
			jdbcTemplate.execute("INSERT INTO elefana_delayed_field_index_queue (_tableName, _fieldName, _timestamp, _generationMode) VALUES ('" +
					tableName + "', '" + fieldName + "', " + indexTimestamp + ", '" + indexGenerationSettings.getMode().name() + "')");
		}
	}

	private void internalCreatePsqlFieldIndex(Connection connection, String tableName, String fieldName, IndexGenerationMode indexGenerationMode) throws SQLException {
		switch(indexGenerationMode) {
		case ALL:
			return;
		case PRESET:
		case DYNAMIC:
			final String btreeIndexName = IndexUtils.BTREE_INDEX_PREFIX + tableName + "_" + fieldName;
			final String createGinFieldIndexQuery = "CREATE INDEX IF NOT EXISTS " + btreeIndexName + " ON " + tableName + " USING BTREE ((_source->>'" + fieldName + "'));";
			LOGGER.info(createGinFieldIndexQuery);
			PreparedStatement preparedStatement = connection.prepareStatement(createGinFieldIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();
			break;
		}
	}

	private void internalCreatePsqlTableIndices(Connection connection, String tableName, IndexGenerationMode indexGenerationMode) throws SQLException {
		final String brinIndexName = IndexUtils.BRIN_INDEX_PREFIX + tableName;
		final String ginIndexName = IndexUtils.GIN_INDEX_PREFIX + tableName;

		PreparedStatement preparedStatement;

		if(indexGenerationMode.equals(IndexGenerationMode.ALL)) {
			final String createGinIndexQuery = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName
					+ " USING GIN (_source jsonb_ops)";
			LOGGER.info(createGinIndexQuery);
			preparedStatement = connection.prepareStatement(createGinIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();
		}

		final String createTimestampIndexQuery = "CREATE INDEX IF NOT EXISTS " + brinIndexName + " ON "
				+ tableName + " USING BRIN (_timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d) WITH (pages_per_range = " + nodeSettingsService.getBrinPagesPerRange() + ")";
		LOGGER.info(createTimestampIndexQuery);
		preparedStatement = connection.prepareStatement(createTimestampIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();
	}
}
