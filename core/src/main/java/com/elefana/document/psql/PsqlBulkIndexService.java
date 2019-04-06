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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.codahale.metrics.Timer;
import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import org.apache.commons.collections.map.LRUMap;
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
	protected ExecutorService executorService;

	private Timer bulkIndexTimer;
	private Counter duplicateKeyCounter;

	@PostConstruct
	public void postConstruct() {
		duplicateKeyCounter = metricRegistry.counter(MetricRegistry.name("bulk", "key", "duplicates"));
		bulkIndexTimer = metricRegistry.timer(MetricRegistry.name("bulk", "index", "duration", "total"));

		final int totalThreads = getTotalThreads();

		executorService = Executors.newFixedThreadPool(totalThreads);
		for (int i = 0; i < totalThreads; i++) {
			executorService.submit(this);
		}

		additionalSetup();
	}

	@PreDestroy
	public void preDestroy() {
		running.set(false);
		executorService.shutdown();

		additionalTeardown();
	}

	protected int getTotalThreads() {
		return Math.max(4, environment.getProperty("elefana.service.bulk.index.threads",
				Integer.class, Runtime.getRuntime().availableProcessors()));
	}

	protected void additionalSetup() {}

	protected void additionalTeardown() {}

	@Override
	public void run() {
		try {
			final Queue<IngestTable> ingestTablesQueue = new LinkedList<IngestTable>();
			while (running.get()) {
				ingestTableTracker.getIngestTables(ingestTablesQueue);

				Connection connection = null;

				while(!ingestTablesQueue.isEmpty()) {
					final IngestTable nextIngestTable = ingestTablesQueue.poll();

					if(connection == null) {
						connection = jdbcTemplate.getDataSource().getConnection();
					}

					try {
						if(ingestTable(connection, nextIngestTable)) {
							if(indexFieldMappingService instanceof PsqlIndexFieldMappingService) {
								((PsqlIndexFieldMappingService) indexFieldMappingService).
										scheduleIndexForMappingAndStats(nextIngestTable.getIndex());
							}
						}
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
						if (connection != null) {
							try {
								connection.close();
								connection = null;
							} catch (SQLException e1) {
								e1.printStackTrace();
							}
						}
					}
				}

				if (connection != null) {
					try {
						connection.close();
						connection = null;
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
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

	protected boolean ingestTable(Connection connection, IngestTable ingestTable) throws SQLException {
		boolean result = false;

		for(int i = 0; i < ingestTable.getCapacity(); i++) {
			final int stagingTableId;
			try {
				stagingTableId = ingestTable.lockWrittenTable();
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				continue;
			}
			if(stagingTableId <= -1) {
				return result;
			}

			final String stagingTableName = ingestTable.getIngestionTableName(stagingTableId);
			final String targetTableName = indexUtils.getPartitionTableForIndex(connection, ingestTable.getIndex());

			BulkIndexResult bulkIndexResult = BulkIndexResult.SUCCESS;
			try {
				final Timer.Context indexTimer = bulkIndexTimer.time();
				if (nodeSettingsService.isUsingCitus()) {
					bulkIndexResult = mergeStagingTableIntoDistributedTable(connection, ingestTable.getIndex(), stagingTableName, targetTableName);
				} else {
					mergeStagingTableIntoPartitionTable(connection, stagingTableName, targetTableName);
				}
				indexTimer.stop();
			} catch (Exception e) {
				if(e.getMessage() != null && e.getMessage().contains("duplicate key")) {
					LOGGER.error("Duplicate key in bulk staging table: " + stagingTableName);
					duplicateKeyCounter.inc();
					bulkIndexResult = BulkIndexResult.DUPLICATE;
				} else {
					LOGGER.error(e.getMessage(), e);
					bulkIndexResult = BulkIndexResult.EXCEPTION;
				}
			}

			try {
				switch(bulkIndexResult) {
				case ROUTE:
					break;
				case EXCEPTION:
					break;
				case DUPLICATE:
					PreparedStatement transferStatement = connection.prepareStatement(
							"INSERT INTO elefana_duplicate_keys SELECT * FROM " + stagingTableName);
					transferStatement.execute();
					transferStatement.close();
					LOGGER.error("Copied " + stagingTableName + " to elefana_duplicate_keys");
				default:
				case SUCCESS: {
					PreparedStatement dropTableStatement = connection.prepareStatement(
							"TRUNCATE TABLE " + stagingTableName);
					dropTableStatement.execute();
					dropTableStatement.close();
					connection.commit();
					ingestTable.unmarkData(stagingTableId);

					result |= true;
					break;
				}
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
			ingestTable.unlockTable(stagingTableId);
		}
		return result;
	}

	protected BulkIndexResult mergeStagingTableIntoDistributedTable(Connection connection, String index, String bulkIngestTable, String targetTable)
			throws ElefanaException, IOException, SQLException {
		if(nodeSettingsService.isMasterNode()) {
			mergeStagingTableIntoPartitionTable(connection, bulkIngestTable, targetTable);
			return BulkIndexResult.SUCCESS;
		}
		LOGGER.info("[Enterprise License Required] Cannot index into " + targetTable + " from non-master node. Data will remain in table " + bulkIngestTable);
		return BulkIndexResult.ROUTE;
	}

	private void mergeStagingTableIntoPartitionTable(Connection connection, String bulkIngestTable, String targetTable) throws IOException, SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " + targetTable + " SELECT * FROM " + bulkIngestTable);
		preparedStatement.executeUpdate();
		preparedStatement.close();
	}

	protected enum BulkIndexResult {
		SUCCESS,
		ROUTE,
		DUPLICATE,
		EXCEPTION
	}
}
