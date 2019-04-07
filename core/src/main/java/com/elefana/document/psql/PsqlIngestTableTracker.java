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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.IndexTimeBucket;
import com.elefana.document.ingest.*;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class PsqlIngestTableTracker implements IngestTableTracker {
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

	protected final ReadWriteLock lock = new ReentrantReadWriteLock();
	protected final Map<String, HashIngestTable> indexToHashIngestTable = new HashMap<String, HashIngestTable>();
	protected final Map<String, TimeIngestTable> indexToTimeIngestTable = new HashMap<String, TimeIngestTable>();

	protected String[] tablespaces;
	protected int defaultCapacity;

	@PostConstruct
	public void postConstruct() throws ElefanaException {
		tablespaces = environment.getProperty("elefana.service.bulk.tablespaces", "").split(",");
		if (isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}

		final int totalIngestThreads = environment.getProperty("elefana.service.bulk.ingest.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalProcessingThreads = (nodeSettingsService.getBulkParallelisation() * totalIngestThreads) + 1;
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

			for(String index : existingTablesByIndex.keySet()) {
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
					} else {
						final HashIngestTable restoredTable = createHashIngestTable(index, existingTablesForIndex);
						indexToHashIngestTable.put(index, restoredTable);
					}
				} else {
					final HashIngestTable restoredTable = createHashIngestTable(index, existingTablesForIndex);
					indexToHashIngestTable.put(index, restoredTable);
				}
			}

			lock.writeLock().unlock();
		}
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
			return new DefaultTimeIngestTable(jdbcTemplate, tablespaces, index, timeBucket, defaultCapacity, existingTables);
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

	private <T> T getIngestTable(String index, Map<String, T> tables, boolean time) throws ElefanaException  {
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
