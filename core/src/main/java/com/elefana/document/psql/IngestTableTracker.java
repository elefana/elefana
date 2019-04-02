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
import com.elefana.node.NodeSettingsService;
import com.elefana.util.CoreIndexUtils;
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
public class IngestTableTracker {
	private static final String[] DEFAULT_TABLESPACES = new String[] { "" };
	private static final List<String> EMPTY_ARRAYLIST = new ArrayList<>(1);

	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<String, IngestTable> indexToIngestTable = new HashMap<String, IngestTable>();

	private String[] tablespaces;
	private int capacity;

	@PostConstruct
	public void postConstruct() throws SQLException {
		tablespaces = environment.getProperty("elefana.service.bulk.tablespaces", "").split(",");
		if (isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}

		final int totalIngestThreads = environment.getProperty("elefana.service.bulk.ingest.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalProcessingThreads = (nodeSettingsService.getBulkParallelisation() * totalIngestThreads) + 1;
		final int totalIndexThreads = Math.max(4, environment.getProperty("elefana.service.bulk.index.threads",
				Integer.class, Runtime.getRuntime().availableProcessors()));
		capacity = totalProcessingThreads + totalIndexThreads;

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
				final IngestTable restoredTable = new IngestTable(jdbcTemplate, tablespaces, index, capacity, existingTablesByIndex.get(index));
				indexToIngestTable.put(index, restoredTable);
			}

			lock.writeLock().unlock();
		}
	}

	public void getIngestTables(Queue<IngestTable> result) {
		lock.readLock().lock();
		result.addAll(indexToIngestTable.values());
		lock.readLock().unlock();
	}

	public IngestTable getIngestTable(String index) throws ElefanaException {
		lock.readLock().lock();
		final IngestTable result = indexToIngestTable.get(index);
		lock.readLock().unlock();

		if(result == null) {
			lock.writeLock().lock();

			final IngestTable newTable;
			if(indexToIngestTable.containsKey(index)) {
				newTable = indexToIngestTable.get(index);
			} else {
				try {
					newTable = new IngestTable(jdbcTemplate, tablespaces, index, capacity, EMPTY_ARRAYLIST);
					indexToIngestTable.put(index, newTable);
				} catch (SQLException e) {
					lock.writeLock().unlock();
					throw new ElefanaException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
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
