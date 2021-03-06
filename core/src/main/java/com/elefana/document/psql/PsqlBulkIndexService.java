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
package com.elefana.document.psql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.document.ingest.HashIngestTable;
import com.elefana.document.ingest.IngestTable;
import com.elefana.document.ingest.IngestTableTracker;
import com.elefana.document.ingest.TimeIngestTable;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.CitusShardMetadataMaintainer;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class PsqlBulkIndexService implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBulkIndexService.class);
	private static final long NOTIFIER_WAIT_MILLIS = 1000L;

	@Autowired
	protected Environment environment;
	@Autowired
	protected JdbcTemplate jdbcTemplate;
	@Autowired
	protected IndexFieldMappingService indexFieldMappingService;
	@Autowired
	protected IndexTemplateService indexTemplateService;
	@Autowired
	protected IndexUtils indexUtils;
	@Autowired
	protected NodeSettingsService nodeSettingsService;
	@Autowired
	protected IngestTableTracker ingestTableTracker;
	@Autowired
	protected MetricRegistry metricRegistry;

	protected final AtomicBoolean running = new AtomicBoolean(true);
	protected final Set<String> routedTables = new ConcurrentSkipListSet<String>();

	protected ExecutorService executorService;
	protected Lock [] shardLocks;

	protected Meter bulkIndexMeter;
	private Timer bulkIndexTimer;
	private Counter duplicateKeyCounter;

	private final Object notifier = new Object();

	@PostConstruct
	public void postConstruct() {
		shardLocks = new Lock[environment.getProperty("elefana.worker.bulk.shard.locks", Integer.class, 100)];
		for(int i = 0; i < shardLocks.length; i++) {
			shardLocks[i] = new ReentrantLock();
		}

		duplicateKeyCounter = metricRegistry.counter(MetricRegistry.name("bulk", "key", "duplicates"));
		bulkIndexTimer = metricRegistry.timer(MetricRegistry.name("bulk", "index", "duration", "total"));
		bulkIndexMeter = metricRegistry.meter(MetricRegistry.name("bulk", "index", "rows"));

		final int totalThreads = getTotalThreads();

		executorService = Executors.newFixedThreadPool(totalThreads, new NamedThreadFactory(
				"elefana-bulkIndexService-indexExecutor", ThreadPriorities.BULK_INDEX_SERVICE));
		additionalSetup();

		for (int i = 0; i < totalThreads; i++) {
			executorService.submit(this);
		}
	}

	@PreDestroy
	public void preDestroy() {
		running.set(false);
		executorService.shutdown();

		try {
			executorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}

		additionalTeardown();
	}

	protected int getTotalThreads() {
		return Math.max(1, environment.getProperty("elefana.service.bulk.index.threads",
				Integer.class, Runtime.getRuntime().availableProcessors()));
	}

	protected void additionalSetup() {}

	protected void additionalTeardown() {}

	@Override
	public void run() {
		try {
			final Queue<HashIngestTable> hashIngestTablesQueue = new LinkedList<HashIngestTable>();
			final Queue<TimeIngestTable> timeIngestTablesQueue = new LinkedList<TimeIngestTable>();
			final AtomicBoolean indexedItems = new AtomicBoolean();
			while (running.get()) {
				indexedItems.set(false);
				ingestTableTracker.getHashIngestTables(hashIngestTablesQueue);
				ingestTableTracker.getTimeIngestTables(timeIngestTablesQueue);

				Connection connection = null;
				connection = processQueue(connection, indexedItems, hashIngestTablesQueue);
				connection = processQueue(connection, indexedItems, timeIngestTablesQueue);

				if (connection != null) {
					try {
						connection.setAutoCommit(true);
						connection.close();
						connection = null;
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}

				if(!indexedItems.get()) {
					//Nothing was indexed, wait until notified content available
					synchronized(notifier) {
						notifier.wait(NOTIFIER_WAIT_MILLIS);
					}
				}

				//Prevent high CPU load
				try {
					Thread.sleep(25L);
				} catch (Exception e) {}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		if (!running.get()) {
			return;
		}
		executorService.submit(this);
	}

	public void notifyContentAvailable() {
		synchronized(notifier) {
			notifier.notifyAll();
		}
	}

	private <T extends IngestTable> Connection processQueue(Connection connection, AtomicBoolean indexedItems, Queue<T> queue) throws SQLException {
		while(!queue.isEmpty()) {
			final IngestTable nextIngestTable = queue.poll();

			if(connection == null) {
				connection = jdbcTemplate.getDataSource().getConnection();
				connection.setAutoCommit(false);
			}

			try {
				if(ingestTable(connection, nextIngestTable)) {
					indexedItems.set(true);
					if(indexFieldMappingService instanceof PsqlIndexFieldMappingService) {
						((PsqlIndexFieldMappingService) indexFieldMappingService).
								scheduleIndexForMappingAndStats(nextIngestTable.getIndex());
					}
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				if (connection != null) {
					try {
						connection.setAutoCommit(true);
						connection.close();
						connection = null;
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
			}
		}
		return connection;
	}

	protected boolean ingestTable(Connection connection, IngestTable ingestTable) throws Exception {
		boolean result = false;

		final IndexTemplate indexTemplate;
		if(indexTemplateService instanceof PsqlIndexTemplateService) {
			indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(ingestTable.getIndex());
		} else {
			indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(null, ingestTable.getIndex()).get().getIndexTemplate();
		}

		for(int i = 0; i < ingestTable.getCapacity(); i++) {
			final int stagingTableId;
			try {
				stagingTableId = ingestTable.lockWrittenTable(routedTables);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				continue;
			}
			if(stagingTableId <= -1) {
				return result;
			}

			String stagingTableName = null;

			BulkIndexResult bulkIndexResult = BulkIndexResult.SUCCESS;
			final Timer.Context indexTimer = bulkIndexTimer.time();
			try {
				stagingTableName = ingestTable.getIngestionTableName(stagingTableId);
				final String targetTableName = indexUtils.getPartitionTableForIndex(connection, ingestTable.getIndex());

				if (nodeSettingsService.isUsingCitus()) {
					bulkIndexResult = mergeStagingTableIntoDistributedTable(connection, indexTemplate,
							ingestTable, stagingTableId, stagingTableName, targetTableName);
				} else {
					mergeStagingTableIntoPartitionTable(connection, stagingTableName, targetTableName);
				}
			} catch (Exception e) {
				if(e.getMessage() != null && e.getMessage().contains("duplicate key") &&
					!e.getMessage().contains("pg_type_")) {
					LOGGER.error("Duplicate key in bulk staging table: " + stagingTableName);
					duplicateKeyCounter.inc();
					bulkIndexResult = BulkIndexResult.DUPLICATE;
				} else {
					LOGGER.error(e.getMessage(), e);
					bulkIndexResult = BulkIndexResult.EXCEPTION;
				}
				connection.rollback();
			}
			indexTimer.stop();

			try {
				switch(bulkIndexResult) {
				case ROUTE:
					routedTables.add(stagingTableName);
					result |= true;
					break;
				case DEFER:
					routedTables.remove(stagingTableName);
					break;
				case EXCEPTION:
					routedTables.remove(stagingTableName);
					ingestTable.unlockTable(stagingTableId);
					return result;
				case DUPLICATE:
					if(nodeSettingsService.isRegenerateDuplicateIds()) {
						PreparedStatement generateIdStatement = connection.prepareStatement(
								"UPDATE " + stagingTableName + " SET _id = ('dup_' || CAST (nextval('elefana_dup_key_id') AS VARCHAR))");
						generateIdStatement.execute();
						generateIdStatement.close();
						connection.commit();
						LOGGER.error("Re-generated ids for " + stagingTableName);
						routedTables.remove(stagingTableName);
						break;
					} else {
						PreparedStatement transferStatement = connection.prepareStatement(
								"INSERT INTO elefana_duplicate_keys SELECT * FROM " + stagingTableName);
						transferStatement.execute();
						transferStatement.close();
						connection.commit();
						LOGGER.error("Copied " + stagingTableName + " to elefana_duplicate_keys");
					}
				default:
				case SUCCESS: {
					bulkIndexMeter.mark(ingestTable.getDataCount(stagingTableId));

					PreparedStatement dropTableStatement = connection.prepareStatement(
							"TRUNCATE TABLE " + stagingTableName);
					dropTableStatement.execute();
					dropTableStatement.close();
					connection.commit();

					ingestTable.unmarkData(stagingTableId);

					routedTables.remove(stagingTableName);
					result |= true;
					break;
				}
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				connection.rollback();
			}
			ingestTable.unlockTable(stagingTableId);
		}
		return result;
	}

	protected BulkIndexResult mergeStagingTableIntoDistributedTable(Connection connection, IndexTemplate indexTemplate, IngestTable ingestTable, int ingestTableEntryIndex, String bulkIngestTable, String targetTable)
			throws ElefanaException, IOException, SQLException {
		if(nodeSettingsService.isMasterNode()) {
			if(indexTemplate != null && indexTemplate.isTimeSeries()) {
				return mergeStagingTableIntoDistributedTimeTable(connection, indexTemplate, ingestTable.getIndex(), bulkIngestTable, targetTable);
			} else {
				return mergeStagingTableIntoPartitionTable(connection, bulkIngestTable, targetTable);
			}
		}
		LOGGER.info("[Enterprise License Required] Cannot index into " + targetTable + " from non-master node. Data will remain in table " + bulkIngestTable);
		return BulkIndexResult.ROUTE;
	}

	protected BulkIndexResult mergeStagingTableIntoDistributedTimeTable(Connection connection, IndexTemplate indexTemplate, String indexName, String bulkIngestTable, String targetTable) throws IOException, SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("SELECT _timestamp FROM " + bulkIngestTable + " LIMIT 1");
		final ResultSet resultSet = preparedStatement.executeQuery();
		if(!resultSet.next()) {
			LOGGER.info("No results in " + bulkIngestTable);
			preparedStatement.close();
			return BulkIndexResult.DEFER;
		}

		final long timestamp = resultSet.getLong("_timestamp");
		preparedStatement.close();
		
		final int shardOffset = indexTemplate.getStorage().getIndexTimeBucket().getShardOffset(timestamp);
		final long shardId = getShardId(connection, targetTable, shardOffset);
		if(shardId == -1) {
			LOGGER.info("No shard available for " + bulkIngestTable);
			return BulkIndexResult.DEFER;
		}
		final int shardLockIndex = (int) (shardId % shardLocks.length);
		try {
			if(shardLocks[shardLockIndex].tryLock(100L, TimeUnit.MILLISECONDS)) {
				try {
					final String query = "SELECT master_append_table_to_shard(" + shardId + ", '" + bulkIngestTable
							+ "', '" + nodeSettingsService.getCitusCoordinatorHost() + "', " + nodeSettingsService.getCitusCoordinatorPort() + ");";
					PreparedStatement appendShardStatement = connection.prepareStatement(query);
					appendShardStatement.execute();
					appendShardStatement.close();
					connection.commit();
					return BulkIndexResult.SUCCESS;
				} catch (Exception e) {
					LOGGER.error("Error indexing " + bulkIngestTable + " to shard " + shardId + " via coordinator " +
							nodeSettingsService.getCitusCoordinatorHost() + ":" +
							nodeSettingsService.getCitusCoordinatorPort() + ". " + e.getMessage(), e);
					return BulkIndexResult.EXCEPTION;
				} finally {
					shardLocks[shardLockIndex].unlock();
				}
			} else {
				return BulkIndexResult.DEFER;
			}
		} catch (InterruptedException e) {
			return BulkIndexResult.EXCEPTION;
		}
	}

	protected BulkIndexResult mergeStagingTableIntoPartitionTable(Connection connection, String bulkIngestTable, String targetTable) throws IOException, SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " + targetTable + " SELECT * FROM " + bulkIngestTable);
		preparedStatement.executeUpdate();
		preparedStatement.close();
		connection.commit();
		return BulkIndexResult.SUCCESS;
	}

	protected long getShardId(Connection connection, String targetTableName, int shardOffset) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("SELECT select_shard('" + targetTableName + "'," + shardOffset + ")");
		final ResultSet shardResultSet = preparedStatement.executeQuery();

		if (!shardResultSet.next()) {
			preparedStatement.close();
			return -1;
		}
		final long shardId = shardResultSet.getLong("select_shard");
		preparedStatement.close();
		return shardId;
	}

	protected enum BulkIndexResult {
		SUCCESS,
		ROUTE,
		DUPLICATE,
		EXCEPTION,
		DEFER
	}
}
