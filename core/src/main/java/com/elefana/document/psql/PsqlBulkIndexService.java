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

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;

@Service
public class PsqlBulkIndexService implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBulkIndexService.class);

	private static final int MAX_FILE_DELETION_RETRIES = 5;
	private static final long SHARD_TIMEOUT = 5000L;

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private PsqlIndexFieldMappingService indexFieldMappingService;
	@Autowired
	private PsqlIndexTemplateService indexTemplateService;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private MetricRegistry metricRegistry;

	private final AtomicBoolean running = new AtomicBoolean(true);
	private final AtomicInteger totalFileDeletions = new AtomicInteger();
	private final Queue<String> fileDeletionQueue = new LinkedList<String>();
	private final AtomicLong lastQueueId = new AtomicLong(Long.MIN_VALUE);
	private Counter duplicateKeyCounter;
	private BlockingQueue<String> indexQueue;
	private ExecutorService executorService;

	private File tmpDirectory;

	@PostConstruct
	public void postConstruct() {
		duplicateKeyCounter = metricRegistry.counter(MetricRegistry.name("bulk", "key", "duplicates"));
		
		final int totalThreads = Math.max(3, environment.getProperty("elefana.service.bulk.index.threads",
				Integer.class, Runtime.getRuntime().availableProcessors()));
		final String tmpDirectoryPath = environment.getProperty("elefana.service.bulk.ingest.dir",
				System.getProperty("java.io.tmpdir"));
		tmpDirectory = new File(tmpDirectoryPath);
		if(!tmpDirectory.exists()) {
			tmpDirectory.mkdirs();
		}

		indexQueue = new ArrayBlockingQueue<String>(totalThreads);
		executorService = Executors.newFixedThreadPool(totalThreads);

		executorService.submit(new Runnable() {

			@Override
			public void run() {
				try {
					final Queue<String> nextIndexTables = new LinkedList<String>();
					while (running.get()) {
						getNextIndexTables(nextIndexTables);

						while (!nextIndexTables.isEmpty()) {
							String nextIndexTable = nextIndexTables.poll();
							indexQueue.put(nextIndexTable);
						}
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
				if (!running.get()) {
					return;
				}
				executorService.submit(this);
			}
		});
		executorService.submit(new Runnable() {

			@Override
			public void run() {
				try {
					while (running.get()) {
						final int totalDeletions = deleteTempFiles();
						if(totalDeletions < 1) {
							synchronized(totalFileDeletions) {
								totalFileDeletions.wait();
							}
						} else {
							totalFileDeletions.addAndGet(totalDeletions);
						}
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
				if (!running.get()) {
					return;
				}
				executorService.submit(this);
			}
		});

		for (int i = 0; i < totalThreads - 2; i++) {
			executorService.submit(this);
		}
	}

	@PreDestroy
	public void preDestroy() {
		running.set(false);
		executorService.shutdown();
	}

	@Override
	public void run() {
		try {
			while (running.get()) {
				final String nextIndexTable = indexQueue.take();
				String index = null;

				Connection connection = null;
				try {
					connection = jdbcTemplate.getDataSource().getConnection();

					try {
						PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM elefana_bulk_index_queue WHERE _tableName='" + nextIndexTable + "'");
						preparedStatement.execute();
						preparedStatement.close();

						index = getIndexName(connection, nextIndexTable);
						if(index != null) {
							final String targetTable = indexUtils.getQueryTarget(connection, index);

							if (nodeSettingsService.isUsingCitus()) {
								mergeStagingTableIntoDistributedTable(connection, index, nextIndexTable, targetTable);
							} else {
								mergeStagingTableIntoPartitionTable(connection, nextIndexTable, targetTable);
							}
						}
					} catch (Exception e) {
						if(e.getMessage() != null && e.getMessage().contains("duplicate key")) {
							LOGGER.error("Duplicate key in bulk staging table: " + nextIndexTable);
							duplicateKeyCounter.inc();
						} else {
							LOGGER.error(e.getMessage(), e);
						}
					}

					PreparedStatement dropTableStatement = connection.prepareStatement("DROP TABLE IF EXISTS " + nextIndexTable);
					dropTableStatement.execute();
					dropTableStatement.close();

					connection.close();
					connection = null;

					if(index != null) {
						indexFieldMappingService.scheduleIndexForMappingAndStats(index);
					}
				} catch (Exception e) {
					if (connection != null) {
						try {
							connection.close();
							connection = null;
						} catch (SQLException e1) {
							e1.printStackTrace();
						}
					}
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		if (!running.get()) {
			return;
		}
		executorService.submit(this);
	}

	private int deleteTempFiles() {
		int result = 0;
		try {
			final long timestamp = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5L);
			final SqlRowSet results = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_file_deletion_queue WHERE _timestamp <= ? ORDER BY _timestamp ASC LIMIT 100", timestamp);
			while(results.next()) {
				final String filepath = results.getString("_filepath");
				fileDeletionQueue.offer(filepath);
			}
			result = fileDeletionQueue.size();

			while(!fileDeletionQueue.isEmpty()) {
				final String filepath = fileDeletionQueue.poll();
				final File file = new File(filepath);
				if(file.exists()) {
					jdbcTemplate.execute("SELECT elefana_delete_tmp_file('" + filepath + "')");
				} else {
					jdbcTemplate.execute("DELETE FROM elefana_file_deletion_queue WHERE _filepath = '" + filepath + "'");
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return result;
	}

	private void getNextIndexTables(final Queue<String> results) {
		final List<Map<String, Object>> nextTableResults = jdbcTemplate
				.queryForList("SELECT * FROM elefana_bulk_index_queue WHERE _queue_id > " + lastQueueId.get());

		for (Map<String, Object> row : nextTableResults) {
			if (row.containsKey("_tableName")) {
				results.offer(((String) row.get("_tableName")).replace('-', '_'));
			}
			if (row.containsKey("_queue_id")) {
				final long queueId = (long) row.get("_queue_id");
				lastQueueId.set(Math.max(lastQueueId.get(), queueId));
			}
		}
	}

	private String getIndexName(Connection connection, String nextIndexTable) throws SQLException {
		final PreparedStatement preparedStatement = connection.prepareStatement("SELECT _index FROM " + nextIndexTable + " LIMIT 1");
		final ResultSet resultSet = preparedStatement.executeQuery();
		String result = null;
		if(resultSet.next()) {
			result = resultSet.getString("_index");
		} else {
			LOGGER.warn("No rows in " + nextIndexTable);
		}
		preparedStatement.close();
		return result;
	}

	private boolean mergeStagingTableIntoDistributedTable(Connection connection, String index, String bulkIngestTable, String targetTable)
			throws ElefanaException, IOException, SQLException {
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(index);

		if (indexTemplate != null && indexTemplate.isTimeSeries()) {
			PreparedStatement preparedStatement = connection.prepareStatement("SELECT select_shard('" + targetTable + "')");
			final ResultSet shardResultSet = preparedStatement.executeQuery();
			preparedStatement.close();

			if (!shardResultSet.next()) {
				return false;
			}
			final long shardId = shardResultSet.getLong("select_shard");

			long timer = 0L;
			Exception lastException = null;
			while (timer < SHARD_TIMEOUT) {
				long startTime = System.currentTimeMillis();
				try {
					PreparedStatement appendShardStatement = connection.prepareStatement(
							"SELECT master_append_table_to_shard(" + shardId + ", '" + bulkIngestTable
							+ "', '" + nodeSettingsService.getCitusCoordinatorHost() + "', "
							+ nodeSettingsService.getCitusCoordinatorPort() + ");");
					preparedStatement.execute();
					preparedStatement.close();
					return true;
				} catch (Exception e) {
					lastException = e;
				}
				try {
					Thread.sleep(100L);
				} catch (Exception e) {
				}
				timer += (System.currentTimeMillis() - startTime);
			}
			lastException.printStackTrace();
			return false;
		} else {
			mergeStagingTableIntoPartitionTable(connection, bulkIngestTable, targetTable);
		}
		return true;
	}

	private void mergeStagingTableIntoPartitionTable(Connection connection, String bulkIngestTable, String targetTable) throws IOException, SQLException {
		String tmpFile = IndexUtils.createTempFilePath("elefana-idx-" + targetTable + "-", ".tmp", tmpDirectory);
		//tmpFile = "/tmp/elefana-idx-" + targetTable + "-" + System.nanoTime() + ".tmp";

		PreparedStatement preparedStatement = connection.prepareStatement("COPY " + bulkIngestTable + " TO '" + tmpFile + "' WITH BINARY ENCODING 'UTF8'");
		preparedStatement.execute();
		preparedStatement.close();

		preparedStatement = connection.prepareStatement("COPY " + targetTable + " FROM '" + tmpFile + "' WITH BINARY ENCODING 'UTF8'");
		preparedStatement.execute();
		preparedStatement.close();

		preparedStatement = connection.prepareStatement("INSERT INTO elefana_file_deletion_queue (_filepath, _timestamp) VALUES ('" + tmpFile + "', " + System.currentTimeMillis() + ")");
		preparedStatement.execute();
		preparedStatement.close();

		synchronized(totalFileDeletions) {
			totalFileDeletions.notifyAll();
		}
	}
}
