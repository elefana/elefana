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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

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
	
	private String previousIndexTable = "";
	private ExecutorService executorService;

	@PostConstruct
	public void postConstruct() {
		executorService = Executors.newFixedThreadPool(1);
		executorService.submit(this);
	}

	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
	}

	@Override
	public void run() {
		try {
			String nextIndexTable = getNextIndexTable();
			while(!previousIndexTable.equals(nextIndexTable)) {
				try {
					final String index = getIndexName(nextIndexTable);
					final String targetTable = indexUtils.getQueryTarget(index);
					
					if(nodeSettingsService.isUsingCitus()) {
						mergeStagingTableIntoDistributedTable(index, nextIndexTable, targetTable);
					} else {
						mergeStagingTableIntoPartitionTable(nextIndexTable, targetTable);
					}
					
					jdbcTemplate.execute("DROP TABLE IF EXISTS " + nextIndexTable);
					jdbcTemplate.execute("DELETE FROM elefana_bulk_index_queue WHERE _tableName='" + nextIndexTable + "'");
					indexFieldMappingService.scheduleIndexForMappingAndStats(index);
					
					previousIndexTable = nextIndexTable;
					nextIndexTable = getNextIndexTable();
				} catch (Exception e) {
					LOGGER.error(e.getMessage() ,e);
					
				}				
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		executorService.submit(this);
	}
	
	private String getNextIndexTable() {
		final List<Map<String, Object>> nextTableResults = jdbcTemplate.queryForList("SELECT _tableName FROM elefana_bulk_index_queue LIMIT 1");
		if (!nextTableResults.isEmpty()) {
			final Map<String, Object> row = nextTableResults.get(0);
			if(row.containsKey("_tableName")) {
				return ((String) row.get("_tableName")).replace('-', '_');
			}
		}
		return previousIndexTable;
	}
	
	private String getIndexName(String nextIndexTable) {
		final List<Map<String, Object>> tableResults = jdbcTemplate.queryForList("SELECT _index FROM " + nextIndexTable + " LIMIT 1");
		if(!tableResults.isEmpty()) {
			final Map<String, Object> row = tableResults.get(0);
			if(row.containsKey("_index")) {
				return ((String) row.get("_index"));
			}
		}
		return null;
	}

	private boolean mergeStagingTableIntoDistributedTable(String index, String bulkIngestTable, String targetTable) throws ElefanaException, IOException {
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(index);

		if (indexTemplate != null && indexTemplate.isTimeSeries()) {
			List<Map<String, Object>> shardResultSet = jdbcTemplate
					.queryForList("SELECT select_shard('" + targetTable + "')");
			if (shardResultSet.isEmpty()) {
				return false;
			}
			long shardId = (long) shardResultSet.get(0).get("select_shard");

			long timer = 0L;
			Exception lastException = null;
			while (timer < SHARD_TIMEOUT) {
				long startTime = System.currentTimeMillis();
				try {
					jdbcTemplate.queryForList("SELECT master_append_table_to_shard(" + shardId + ", '"
							+ bulkIngestTable + "', '" + nodeSettingsService.getCitusCoordinatorHost()
							+ "', " + nodeSettingsService.getCitusCoordinatorPort() + ");");
					LOGGER.info(bulkIngestTable + " appended to shard " + shardId);
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
			mergeStagingTableIntoPartitionTable(bulkIngestTable, targetTable);
		}
		return true;
	}

	private void mergeStagingTableIntoPartitionTable(String bulkIngestTable, String targetTable) throws IOException {
		String tmpFile = "/tmp/elefana-idx-" + targetTable + "-" + System.nanoTime() + ".tmp";
		jdbcTemplate.execute("COPY " + bulkIngestTable + " TO '" + tmpFile + "' WITH BINARY");
		jdbcTemplate.execute("COPY " + targetTable + " FROM '" + tmpFile + "' WITH BINARY");
		jdbcTemplate.queryForList("SELECT elefana_delete_tmp_file('" + tmpFile + "')");
	}
}
