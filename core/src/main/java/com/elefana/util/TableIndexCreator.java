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
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;

@Service
@DependsOn("nodeInfoService")
public class TableIndexCreator implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableIndexCreator.class);

	private final DelayQueue<DelayedTableIndexCreation> tableIndexCreationQueue = new DelayQueue<DelayedTableIndexCreation>();
	private final DelayQueue<DelayedFieldIndexCreation> fieldIndexCreationQueue = new DelayQueue<DelayedFieldIndexCreation>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

	@Autowired
	private Environment environment;
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
		scheduledExecutorService.scheduleAtFixedRate(this, 1L, Math.max(1000, interval), TimeUnit.MILLISECONDS);
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
		DelayedTableIndexCreation nextIndexCreation = tableIndexCreationQueue.poll();
		while(nextIndexCreation != null) {
			if(connection == null) {
				connection = jdbcTemplate.getDataSource().getConnection();
			}
			internalCreatePsqlIndices(connection, nextIndexCreation.tableName, nextIndexCreation.indexGenerationSettings);
			nextIndexCreation = tableIndexCreationQueue.poll();
		}
		return connection;
	}

	private Connection runFieldIndexCreation(Connection connection) throws SQLException {
		DelayedFieldIndexCreation nextIndexCreation = fieldIndexCreationQueue.poll();
		while(nextIndexCreation != null) {
			if(connection == null) {
				connection = jdbcTemplate.getDataSource().getConnection();
			}
			internalCreatePsqlIndex(connection, nextIndexCreation.tableName, nextIndexCreation.fieldName, nextIndexCreation.indexGenerationSettings);
			nextIndexCreation = fieldIndexCreationQueue.poll();
		}
		return connection;
	}

	public void createPsqlIndices(Connection connection, String tableName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		if(indexGenerationSettings.getIndexDelaySeconds() <= 0) {
			internalCreatePsqlIndices(connection, tableName, indexGenerationSettings);
		} else {
			final DelayedTableIndexCreation delayedTableIndexCreation = new DelayedTableIndexCreation();
			delayedTableIndexCreation.indexGenerationSettings = indexGenerationSettings;
			delayedTableIndexCreation.tableName = tableName;
			delayedTableIndexCreation.delayMillis = TimeUnit.SECONDS.toMillis(indexGenerationSettings.getIndexDelaySeconds());
			tableIndexCreationQueue.offer(delayedTableIndexCreation);
		}
	}

	public void createPsqlIndex(Connection connection, String tableName, String fieldName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		if(indexGenerationSettings.getIndexDelaySeconds() <= 0) {
			internalCreatePsqlIndex(connection, tableName, fieldName, indexGenerationSettings);
		} else {
			final DelayedFieldIndexCreation delayedFieldIndexCreation = new DelayedFieldIndexCreation();
			delayedFieldIndexCreation.indexGenerationSettings = indexGenerationSettings;
			delayedFieldIndexCreation.tableName = tableName;
			delayedFieldIndexCreation.fieldName = fieldName;
			delayedFieldIndexCreation.delayMillis = TimeUnit.SECONDS.toMillis(indexGenerationSettings.getIndexDelaySeconds());
			fieldIndexCreationQueue.offer(delayedFieldIndexCreation);
		}
	}

	private void internalCreatePsqlIndex(Connection connection, String tableName, String fieldName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		switch(indexGenerationSettings.getMode()) {
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

	private void internalCreatePsqlIndices(Connection connection, String tableName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		final String brinIndexName = IndexUtils.BRIN_INDEX_PREFIX + tableName;
		final String ginIndexName = IndexUtils.GIN_INDEX_PREFIX + tableName;

		PreparedStatement preparedStatement;

		if(indexGenerationSettings.getMode().equals(IndexGenerationMode.ALL)) {
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

	private class DelayedTableIndexCreation implements Delayed {
		public String tableName;
		public IndexGenerationSettings indexGenerationSettings;
		public long delayMillis;

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(Delayed o) {
			return Long.compare(delayMillis, o.getDelay(TimeUnit.MILLISECONDS));
		}
	}

	private class DelayedFieldIndexCreation implements Delayed {
		public String tableName;
		public String fieldName;
		public IndexGenerationSettings indexGenerationSettings;
		public long delayMillis;

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(Delayed o) {
			return Long.compare(delayMillis, o.getDelay(TimeUnit.MILLISECONDS));
		}
	}
}
