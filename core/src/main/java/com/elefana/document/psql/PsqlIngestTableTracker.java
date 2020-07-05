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
package com.elefana.document.psql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.IndexTimeBucket;
import com.elefana.document.ingest.*;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class PsqlIngestTableTracker implements IngestTableTracker, Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlIngestTableTracker.class);

	private static final String[] DEFAULT_TABLESPACES = new String[] { "" };
	private static final List<String> EMPTY_ARRAYLIST = new ArrayList<>(1);

	@Autowired
	protected Environment environment;
	@Autowired
	protected NodeSettingsService nodeSettingsService;
	@Autowired
	protected JdbcTemplate jdbcTemplate;
	@Autowired
	protected IndexTemplateService indexTemplateService;
	@Autowired
	protected TaskScheduler taskScheduler;
	@Autowired
	protected MetricRegistry metricRegistry;

	protected final ReadWriteLock lock = new ReentrantReadWriteLock();
	protected final Map<String, HashIngestTable> indexToHashIngestTable = new ConcurrentHashMap<String, HashIngestTable>();
	protected final Map<String, TimeIngestTable> indexToTimeIngestTable = new ConcurrentHashMap<String, TimeIngestTable>();

	protected String[] tablespaces;
	protected int defaultCapacity;
	protected long ingestionTableExpiryMillis;

	private Counter ingestTableCounter;

	@PostConstruct
	public void postConstruct() throws ElefanaException {
		ingestTableCounter = metricRegistry.counter(MetricRegistry.name("bulk", "ingest", "tables"));

		ingestionTableExpiryMillis = environment.getProperty("elefana.service.bulk.ingest.tableExpiryMillis", Long.class, TimeUnit.HOURS.toMillis(6L));

		tablespaces = environment.getProperty("elefana.service.bulk.tablespaces", "").split(",");
		if (isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}

		final int totalIngestThreads = environment.getProperty("elefana.service.bulk.ingest.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalProcessingThreads = (nodeSettingsService.getBulkParallelisation() * totalIngestThreads) + (nodeSettingsService.getBulkParallelisation() * 2);
		final int totalIndexThreads = Math.max(4, environment.getProperty("elefana.service.bulk.index.threads",
				Integer.class, Runtime.getRuntime().availableProcessors()));
		defaultCapacity = totalProcessingThreads + totalIndexThreads;

		final List<Map<String, Object>> existingTables = jdbcTemplate.queryForList("SELECT * FROM elefana_bulk_tables");
		if(!existingTables.isEmpty()) {
			lock.writeLock().lock();

			final Map<String, List<String>> existingTablesByIndex = new HashMap<String, List<String>>();
			for(Map<String, Object> row : existingTables) {
				final String index = (String) row.get("_index");
				final String tableName = (String) row.get("_ingestTableName");

				if(!existingTablesByIndex.containsKey(index)) {
					existingTablesByIndex.put(index, new ArrayList<String>());
				}
				existingTablesByIndex.get(index).add(tableName);
			}

			final AtomicReference<ElefanaException> exception = new AtomicReference<ElefanaException>();
			existingTablesByIndex.keySet().parallelStream().forEach(index -> {
				try {
					final List<String> existingTablesForIndex = existingTablesByIndex.get(index);

					if(nodeSettingsService.isUsingCitus()) {
						final IndexTemplate indexTemplate;
						if(indexTemplateService instanceof PsqlIndexTemplateService) {
							indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(index);
						} else {
							indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(index).get().getIndexTemplate();
						}

						if(indexTemplate != null && indexTemplate.isTimeSeries()) {
							final TimeIngestTable restoredTable = createTimeIngestTable(index, indexTemplate.getStorage().getIndexTimeBucket(), existingTablesForIndex);
							indexToTimeIngestTable.put(index, restoredTable);
							ingestTableCounter.inc(restoredTable.getCapacity());
						} else {
							final HashIngestTable restoredTable = createHashIngestTable(index, existingTablesForIndex);
							indexToHashIngestTable.put(index, restoredTable);
							ingestTableCounter.inc(restoredTable.getCapacity());
						}
					} else {
						final HashIngestTable restoredTable = createHashIngestTable(index, existingTablesForIndex);
						indexToHashIngestTable.put(index, restoredTable);
						ingestTableCounter.inc(restoredTable.getCapacity());
					}
				} catch (ElefanaException e) {
					exception.set(e);
				}
			});

			if(exception.get() != null) {
				throw exception.get();
			}

			lock.writeLock().unlock();
		}

		taskScheduler.scheduleAtFixedRate(this, Instant.now().plus(ingestionTableExpiryMillis, ChronoUnit.MILLIS),
				Duration.ofMillis(ingestionTableExpiryMillis));
	}

	@Override
	public void run() {
		final long timestamp = System.currentTimeMillis();
		lock.readLock().lock();
		try {
			for(String key : indexToHashIngestTable.keySet()) {
				final HashIngestTable hashIngestTable = indexToHashIngestTable.get(key);
				if(hashIngestTable == null) {
					continue;
				}
				if(timestamp - hashIngestTable.getLastUsageTimestamp() < ingestionTableExpiryMillis) {
					LOGGER.info(key + ", Last Time Used: " + hashIngestTable.getLastUsageTimestamp() + ", Now: " + timestamp + " (HASH TABLE) " + ingestTableCounter.getCount());
					continue;
				}
				lock.readLock().unlock();

				if(hashIngestTable.prune()) {
					LOGGER.info(key + " pre-pruned " + ingestTableCounter.getCount());
					lock.writeLock().lock();
					indexToHashIngestTable.remove(key);
					ingestTableCounter.dec(hashIngestTable.getCapacity());
					lock.writeLock().unlock();
					LOGGER.info(key + " pruned " + ingestTableCounter.getCount());
				}

				lock.readLock().lock();
			}
			for(String key : indexToTimeIngestTable.keySet()) {
				final TimeIngestTable timeIngestTable = indexToTimeIngestTable.get(key);
				if(timeIngestTable == null) {
					continue;
				}
				if(timestamp - timeIngestTable.getLastUsageTimestamp() < ingestionTableExpiryMillis) {
					LOGGER.info(key + ", Last Time Used: " + timeIngestTable.getLastUsageTimestamp() + ", Now: " + timestamp + " (TIME TABLE) " + ingestTableCounter.getCount());
					continue;
				}
				lock.readLock().unlock();

				if(timeIngestTable.prune()) {
					LOGGER.info(key + " pre-pruned " + ingestTableCounter.getCount());
					lock.writeLock().lock();
					indexToTimeIngestTable.remove(key);
					ingestTableCounter.dec(timeIngestTable.getCapacity());
					lock.writeLock().unlock();

					LOGGER.info(key + " pruned " + ingestTableCounter.getCount());
				}

				lock.readLock().lock();
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		lock.readLock().unlock();
	}

	protected TimeIngestTable createTimeIngestTable(String index, List<String> existingTables) throws ElefanaException {
		final IndexTemplate indexTemplate;
		if(indexTemplateService instanceof PsqlIndexTemplateService) {
			indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(index);
		} else {
			indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(index).get().getIndexTemplate();
		}
		return createTimeIngestTable(index, indexTemplate.getStorage().getIndexTimeBucket(), existingTables);
	}

	protected TimeIngestTable createTimeIngestTable(String index, IndexTimeBucket timeBucket, List<String> existingTables) throws ElefanaException {
		try {
			return new DefaultTimeIngestTable(jdbcTemplate, tablespaces, index, timeBucket, nodeSettingsService.getBulkParallelisation(), existingTables);
		} catch (SQLException e) {
			throw new ElefanaException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
		}
	}

	protected HashIngestTable createHashIngestTable(String index, List<String> existingTables) throws ElefanaException {
		try {
			return new DefaultHashIngestTable(jdbcTemplate, tablespaces, index, defaultCapacity, existingTables);
		} catch (SQLException e) {
			throw new ElefanaException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
		}
	}

	public void getHashIngestTables(Queue<HashIngestTable> result) {
		lock.readLock().lock();
		result.addAll(indexToHashIngestTable.values());
		lock.readLock().unlock();
	}

	public void getTimeIngestTables(Queue<TimeIngestTable> result) {
		lock.readLock().lock();
		result.addAll(indexToTimeIngestTable.values());
		lock.readLock().unlock();
	}

	public HashIngestTable getHashIngestTable(String index) throws ElefanaException {
		return getIngestTable(index, indexToHashIngestTable, false);
	}

	@Override
	public TimeIngestTable getTimeIngestTable(String index) throws ElefanaException {
		return getIngestTable(index, indexToTimeIngestTable, true);
	}

	@Override
	public int getTotalIngestTables() {
		int result = 0;
		lock.readLock().lock();
		try {
			for(String index: indexToHashIngestTable.keySet()) {
				result += indexToHashIngestTable.get(index).getCapacity();
			}
			for(String index: indexToTimeIngestTable.keySet()) {
				result += indexToTimeIngestTable.get(index).getCapacity();
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		lock.readLock().unlock();
		return result;
	}

	private <T extends IngestTable> T getIngestTable(String index, Map<String, T> tables, boolean time) throws ElefanaException  {
		lock.readLock().lock();
		final T result = tables.get(index);
		lock.readLock().unlock();

		if(result == null) {
			lock.writeLock().lock();

			final T newTable;
			if(tables.containsKey(index)) {
				newTable = tables.get(index);
			} else {
				try {
					if(nodeSettingsService.isUsingCitus() && time) {
						newTable = (T) createTimeIngestTable(index, EMPTY_ARRAYLIST);
					} else {
						newTable = (T) createHashIngestTable(index, EMPTY_ARRAYLIST);
					}
					ingestTableCounter.inc(newTable.getCapacity());
					tables.put(index, newTable);
				} catch (ElefanaException e) {
					lock.writeLock().unlock();
					throw e;
				}
			}

			lock.writeLock().unlock();
			return newTable;
		}
		return result;
	}

	private boolean isEmptyTablespaceList(String[] tablespaces) {
		if (tablespaces == null) {
			return true;
		}
		for (int i = 0; i < tablespaces.length; i++) {
			if (tablespaces[i] == null) {
				continue;
			}
			if (tablespaces[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}
}
