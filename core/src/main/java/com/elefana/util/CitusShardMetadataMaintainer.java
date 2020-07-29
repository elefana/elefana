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

	private static final String TIME_SHARD_REPAIR_QUEUE_ID = "time-shard-repair-queue";
	private static final int TIME_SHARD_REPAIR_EXPECTED_ENTRIES = 100_000;
	private static final String TIME_SHARD_REPAIR_AVERAGE_KEY = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
	private static final CitusTableTimestampSample TIME_SHARD_REPAIR_AVERAGE_VALUE =
			new CitusTableTimestampSample(TIME_SHARD_REPAIR_AVERAGE_KEY, TIME_SHARD_REPAIR_AVERAGE_KEY,
					System.currentTimeMillis());

	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	protected IndexTemplateService indexTemplateService;

	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

	private HashDiskBackedQueue<CitusTableTimestampSample> timeShardRepairQueue;

	@PostConstruct
	public void postConstruct() throws SQLException {
		if(!nodeSettingsService.isMasterNode()) {
			return;
		}
		timeShardRepairQueue = new HashDiskBackedQueue<CitusTableTimestampSample>(TIME_SHARD_REPAIR_QUEUE_ID,
				nodeSettingsService.getDataDirectory(), CitusTableTimestampSample.class,
				TIME_SHARD_REPAIR_EXPECTED_ENTRIES,
				TIME_SHARD_REPAIR_AVERAGE_KEY, TIME_SHARD_REPAIR_AVERAGE_VALUE);
		executorService.scheduleAtFixedRate(this, 0L, environment.getProperty("elefana.citus.repair.interval", Long.class, DEFAULT_SCHEDULE_MILLIS), TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeSettingsService.isMasterNode()) {
			return;
		}
		executorService.shutdownNow();
		timeShardRepairQueue.dispose();
	}

	public void queueTimeSeriesIndexForShardMaintenance(String index, String tableName, long timestampSample) {
		if(!nodeSettingsService.isMasterNode()) {
			return;
		}
		timeShardRepairQueue.offer(new CitusTableTimestampSample(index, tableName, timestampSample));
	}

	@Override
	public void run() {
		if(timeShardRepairQueue.isEmpty()) {
			LOGGER.info("No shards queued for repair");
			return;
		}

		final CitusTableTimestampSample tableTimestampSample = new CitusTableTimestampSample();
		while(!timeShardRepairQueue.isEmpty()) {
			try {
				if(!timeShardRepairQueue.poll(tableTimestampSample)) {
					continue;
				}
				if(hasNullShardIntervals(tableTimestampSample)) {
					repairShardIntervals(tableTimestampSample);
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				return;
			}
		}
		timeShardRepairQueue.prune();
	}

	private boolean hasNullShardIntervals(CitusTableTimestampSample tableTimestampSample) {
		final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT COUNT(*) FROM (SELECT logicalrelid::text AS tableName, * FROM pg_dist_shard) AS results WHERE tableName='" +
				tableTimestampSample.getTableName() + "' AND shardmaxvalue IS NULL");
		if(rowSet.next()) {
			if(rowSet.getLong(1) > 0) {
				return true;
			}
		}
		LOGGER.info(tableTimestampSample.getTableName() + " shards in correct state");
		return false;
	}

	private void repairShardIntervals(CitusTableTimestampSample tableTimestampSample) throws Exception {
		final IndexTemplate indexTemplate;
		if(indexTemplateService instanceof PsqlIndexTemplateService) {
			indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(tableTimestampSample.getIndexName());
		} else {
			indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(tableTimestampSample.getIndexName()).get().getIndexTemplate();
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
				tableTimestampSample.getTableName() + "' ORDER BY shardid ASC;");
		while(rowSet.next()) {
			shardIds.add(rowSet.getLong(1));
		}

		if(shardIds.size() != timeBucket.getIngestTableCapacity()) {
			throw new Exception("Shard count mismatch for " + tableTimestampSample.getTableName() +
					". Expected " + timeBucket.getIngestTableCapacity() + " but got " + shardIds.size());
		}

		final long indexStart = tableTimestampSample.getTimestampSample() -
				(tableTimestampSample.getTimestampSample() % timeBucket.getBucketOperand());
		final long interval = timeBucket.getBucketInterval();

		for(int i = 0; i < shardIds.size(); i++) {
			final long shardId = shardIds.get(i);
			final long minValue = indexStart + (interval * i);
			final long maxValue = minValue + interval - 1L;

			jdbcTemplate.execute("UPDATE pg_dist_shard SET shardminvalue='" + minValue + "', shardmaxvalue='" + maxValue + "' WHERE shardid=" + shardId);
		}

		LOGGER.info("Repaired shard intervals for " + tableTimestampSample.getTableName());
	}
}
