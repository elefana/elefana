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
package com.elefana.util;

import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.IndexTimeBucket;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class CitusShardMetadataMaintainer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(CitusShardMetadataMaintainer.class);
	private static final long DEFAULT_SCHEDULE_MILLIS = TimeUnit.HOURS.toMillis(1);

	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	protected IndexTemplateService indexTemplateService;

	private ScheduledExecutorService executorService;

	@PostConstruct
	public void postConstruct() throws SQLException {
		if(!nodeSettingsService.isMasterNode()) {
			return;
		}
		executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("citusShardMaintainer", ThreadPriorities.CITUS_SHARD_MAINTAINER));
		executorService.scheduleAtFixedRate(this, 0L, environment.getProperty("elefana.citus.repair.interval", Long.class, DEFAULT_SCHEDULE_MILLIS), TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeSettingsService.isMasterNode()) {
			return;
		}
		executorService.shutdownNow();
	}

	@Override
	public void run() {
		try {
			final IndexTablePair tableTimestampSample = new IndexTablePair();

			final SqlRowSet partitionTables = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_partition_tracking");
			while(partitionTables.next()) {
				try {
					tableTimestampSample.setIndexName(partitionTables.getString("_index"));
					tableTimestampSample.setTableName(partitionTables.getString("_partitiontable"));

					if(hasNullShardIntervals(tableTimestampSample)) {
						repairShardIntervals(tableTimestampSample);
					} else if(hasOverlappingShards(tableTimestampSample)) {
						//TODO: Attempt data correction?
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private boolean hasOverlappingShards(IndexTablePair tablePair) {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM (SELECT logicalrelid::text AS tableName, * FROM pg_dist_shard) AS results WHERE tableName='" +
				tablePair.getTableName() + "' ORDER BY shardminvalue ASC");
		long previousMaxValue = -1;

		while(rowSet.next()) {
			final long shardMaxValue = rowSet.getLong("shardmaxvalue");
			if(previousMaxValue == -1) {
				previousMaxValue = shardMaxValue;
				continue;
			}
			final long shardMinValue = rowSet.getLong("shardminvalue");
			if(previousMaxValue < shardMinValue) {
				previousMaxValue = shardMaxValue;
				continue;
			}
			LOGGER.error(tablePair.getTableName() + " has overlapping shard ranges: " + shardMinValue + " " + shardMaxValue + " " + previousMaxValue);
			return true;
		}
		return false;
	}

	private boolean hasNullShardIntervals(IndexTablePair tablePair) {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT COUNT(*) FROM (SELECT logicalrelid::text AS tableName, * FROM pg_dist_shard) AS results WHERE tableName='" +
				tablePair.getTableName() + "' AND shardmaxvalue IS NULL");
		if(rowSet.next()) {
			if(rowSet.getLong(1) > 0) {
				return true;
			}
		}
		LOGGER.info(tablePair.getTableName() + " no null shards");
		return false;
	}

	private void repairShardIntervals(IndexTablePair tablePair) throws Exception {
		final IndexTemplate indexTemplate;
		if(indexTemplateService instanceof PsqlIndexTemplateService) {
			indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(tablePair.getIndexName());
		} else {
			indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(null, tablePair.getIndexName()).get().getIndexTemplate();
		}

		if(!indexTemplate.isTimeSeries()) {
			return;
		}

		final IndexTimeBucket timeBucket;
		if(indexTemplate.getStorage() != null && indexTemplate.getStorage().getIndexTimeBucket() != null) {
			timeBucket = indexTemplate.getStorage().getIndexTimeBucket();
		} else {
			timeBucket = IndexTimeBucket.MINUTE;
		}

		final List<Long> shardIds = new ArrayList<Long>();
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT shardid FROM (SELECT logicalrelid::text AS tableName, * FROM pg_dist_shard) AS results WHERE tableName='" +
				tablePair.getTableName() + "' ORDER BY shardid ASC;");
		while(rowSet.next()) {
			shardIds.add(rowSet.getLong(1));
		}

		if(shardIds.size() != timeBucket.getIngestTableCapacity()) {
			throw new Exception("Shard count mismatch for " + tablePair.getTableName() +
					". Expected " + timeBucket.getIngestTableCapacity() + " but got " + shardIds.size());
		}

		final SqlRowSet timestampSample = jdbcTemplate.queryForRowSet("SELECT _timestamp FROM " + tablePair.getTableName() + " LIMIT 1");
		if(!timestampSample.next()) {
			return;
		}
		final long timestamp = timestampSample.getLong("_timestamp");
		final long indexStart = timestamp - (timestamp % timeBucket.getBucketOperand());
		final long interval = timeBucket.getBucketInterval();

		for(int i = 0; i < shardIds.size(); i++) {
			final long shardId = shardIds.get(i);
			final long minValue = indexStart + (interval * i);
			final long maxValue = minValue + interval - 1L;

			jdbcTemplate.execute("UPDATE pg_dist_shard SET shardminvalue='" + minValue + "', shardmaxvalue='" + maxValue + "' WHERE shardid=" + shardId);
		}

		LOGGER.info("Repaired shard intervals for " + tablePair.getTableName());
	}
}
