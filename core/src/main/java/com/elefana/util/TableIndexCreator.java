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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@DependsOn("nodeInfoService")
public class TableIndexCreator implements Runnable {
	private static final String DEFAULT_INDEX_CREATION_DELAY = "0";
	private static final String DEFAULT_BRIN_PAGES_PER_RANGE = "128";
	private static final Logger LOGGER = LoggerFactory.getLogger(TableIndexCreator.class);

	private final DelayQueue<DelayedIndexCreation> indexCreationQueue = new DelayQueue<DelayedIndexCreation>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
	private final AtomicBoolean running = new AtomicBoolean(true);

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeInfoService nodeInfoService;

	private long delayPeriodMillis;
	private int brinPagesPerRange;

	@PostConstruct
	public void postConstruct() throws SQLException {
		brinPagesPerRange = Integer.parseInt(environment.getProperty("elefana.brinPagesPerRange", DEFAULT_BRIN_PAGES_PER_RANGE));

		if(!nodeInfoService.isMasterNode()) {
			//Only master node can create indices
			scheduledExecutorService.shutdown();
			return;
		}

		delayPeriodMillis = Integer.parseInt(environment.getProperty("elefana.psqlIndexCreationDelay", DEFAULT_INDEX_CREATION_DELAY));
		if(delayPeriodMillis <= 0) {
			scheduledExecutorService.shutdown();
			return;
		}
		scheduledExecutorService.scheduleAtFixedRate(this, 1L, delayPeriodMillis, TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		running.set(false);

		if(delayPeriodMillis <= 0) {
			return;
		}
		scheduledExecutorService.shutdown();
	}

	@Override
	public void run() {
		while(running.get()) {
			final DelayedIndexCreation nextIndexCreation = indexCreationQueue.poll();
			if(nextIndexCreation == null) {
				return;
			}

			Connection connection = null;
			try {
				connection = jdbcTemplate.getDataSource().getConnection();

				internalCreatePsqlIndices(connection, nextIndexCreation.tableName, nextIndexCreation.indexGenerationSettings);
				indexCreationQueue.poll();

				connection.close();
			} catch (Exception e) {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
				e.printStackTrace();
			}
		}
	}

	public void createPsqlIndices(Connection connection, String tableName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		if(delayPeriodMillis <= 0) {
			internalCreatePsqlIndices(connection, tableName, indexGenerationSettings);
		} else {
			DelayedIndexCreation delayedIndexCreation = new DelayedIndexCreation();
			delayedIndexCreation.indexGenerationSettings = indexGenerationSettings;
			delayedIndexCreation.tableName = tableName;
			delayedIndexCreation.delayMillis = delayPeriodMillis;
			indexCreationQueue.offer(delayedIndexCreation);
		}
	}

	public void createPsqlIndex(Connection connection, String tableName, String fieldName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		switch(indexGenerationSettings.getMode()) {
		case ALL:
			return;
		case PRESET:
		case DYNAMIC:
			final String ginIndexName = IndexUtils.GIN_INDEX_PREFIX + tableName + "_" + fieldName;
			final String createGinFieldIndexQuery = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName + " USING BTREE ((_source->>'" + fieldName + "'));";
			LOGGER.info(createGinFieldIndexQuery);
			PreparedStatement preparedStatement = connection.prepareStatement(createGinFieldIndexQuery);
			preparedStatement.execute();
			preparedStatement.close();
			break;
		}
	}

	private void internalCreatePsqlIndices(Connection connection, String tableName, IndexGenerationSettings indexGenerationSettings) throws SQLException {
		final String timestampIndexName = IndexUtils.TIMESTAMP_INDEX_PREFIX + tableName;
		final String bucket1sIndexName = IndexUtils.SECOND_INDEX_PREFIX + tableName;
		final String bucket1mIndexName = IndexUtils.MINUTE_INDEX_PREFIX + tableName;
		final String bucket1hIndexName = IndexUtils.HOUR_INDEX_PREFIX + tableName;
		final String bucket1dIndexName = IndexUtils.DAY_INDEX_PREFIX + tableName;
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

		final String createTimestampIndexQuery = "CREATE INDEX IF NOT EXISTS " + timestampIndexName + " ON "
				+ tableName + " USING BRIN (_timestamp) WITH (pages_per_range = " + brinPagesPerRange + ")";
		LOGGER.info(createTimestampIndexQuery);
		preparedStatement = connection.prepareStatement(createTimestampIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createBucket1sIndexQuery = "CREATE INDEX IF NOT EXISTS " + bucket1sIndexName + " ON "
				+ tableName + " USING BRIN (_bucket1s) WITH (pages_per_range = " + brinPagesPerRange + ")";
		LOGGER.info(createBucket1sIndexQuery);
		preparedStatement = connection.prepareStatement(createBucket1sIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createBucket1mIndexQuery = "CREATE INDEX IF NOT EXISTS " + bucket1mIndexName + " ON "
				+ tableName + " USING BRIN (_bucket1m) WITH (pages_per_range = " + brinPagesPerRange + ")";
		LOGGER.info(createBucket1mIndexQuery);
		preparedStatement = connection.prepareStatement(createBucket1mIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createBucket1hIndexQuery = "CREATE INDEX IF NOT EXISTS " + bucket1hIndexName + " ON "
				+ tableName + " USING BRIN (_bucket1h) WITH (pages_per_range = " + brinPagesPerRange + ")";
		LOGGER.info(createBucket1hIndexQuery);
		preparedStatement = connection.prepareStatement(createBucket1hIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createBucket1dIndexQuery = "CREATE INDEX IF NOT EXISTS " + bucket1dIndexName + " ON "
				+ tableName + " USING BRIN (_bucket1d) WITH (pages_per_range = " + brinPagesPerRange + ")";
		LOGGER.info(createBucket1dIndexQuery);
		preparedStatement = connection.prepareStatement(createBucket1dIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();
	}

	private class DelayedIndexCreation implements Delayed {
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
}
