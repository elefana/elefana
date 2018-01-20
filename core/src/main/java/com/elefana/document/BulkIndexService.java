/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.document;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplate;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;

@Service
public class BulkIndexService implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkIndexService.class);
	
	private static final long SHARD_TIMEOUT = 5000L;

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private MetricRegistry metricRegistry;

	private final Queue<IndexTarget> indexQueue = new ConcurrentLinkedQueue<IndexTarget>();

	private String jdbcHost;
	private int jdbcPort;
	private ExecutorService executorService;

	@PostConstruct
	public void postConstruct() {
		String jdbcUrl = environment.getRequiredProperty("spring.datasource.url").substring(5);
		URI jdbcUri = URI.create(jdbcUrl);
		jdbcHost = jdbcUri.getHost();
		jdbcPort = jdbcUri.getPort();

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
			while (!indexQueue.isEmpty()) {
				final IndexTarget indexTarget = indexQueue.poll();
				if (nodeSettingsService.isUsingCitus()) {
					if(!mergeStagingTableIntoDistributedTable(indexTarget)) {
						indexQueue.offer(indexTarget);
						continue;
					}
				} else {
					mergeStagingTableIntoPartitionTable(indexTarget);
				}
				jdbcTemplate.execute("DROP TABLE IF EXISTS " + indexTarget.getStagingTable());
				indexFieldMappingService.scheduleIndexForMappingAndStats(indexTarget.getIndex());
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		executorService.submit(this);
	}

	public void queue(IndexTarget indexTarget) {
		indexQueue.offer(indexTarget);
	}

	private boolean mergeStagingTableIntoDistributedTable(IndexTarget indexTarget) {
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(indexTarget.getIndex());

		if (indexTemplate != null && indexTemplate.isTimeSeries()) {
			List<Map<String, Object>> shardResultSet = jdbcTemplate
					.queryForList("SELECT select_shard('" + indexTarget.getTargetTable() + "')");
			if(shardResultSet.isEmpty()) {
				return false;
			}
			long shardId = (long) shardResultSet.get(0).get("select_shard");
			
			long timer = 0L;
			Exception lastException = null;
			while(timer < SHARD_TIMEOUT) {
				long startTime = System.currentTimeMillis();
				try {
					jdbcTemplate.queryForList("SELECT master_append_table_to_shard(" + shardId + ", '"
							+ indexTarget.getStagingTable() + "', '" + jdbcHost + "', " + jdbcPort + ");");
					LOGGER.info(indexTarget.getStagingTable() + " appended to shard " + shardId);
					return true;
				} catch (Exception e) {
					lastException = e;
				}
				try {
					Thread.sleep(100L);
				} catch (Exception e) {}
				timer += (System.currentTimeMillis() - startTime);
			}
			lastException.printStackTrace();
			return false;
		} else {
			jdbcTemplate.update("INSERT INTO " + indexTarget.getTargetTable()
					+ "(_index, _type, _id, _timestamp, _source) select _index, _type, _id, _timestamp, _source FROM "
					+ indexTarget.getStagingTable());
		}
		return true;
	}

	private void mergeStagingTableIntoPartitionTable(IndexTarget indexTarget) {
		jdbcTemplate.update("INSERT INTO " + indexTarget.getTargetTable()
				+ "(_index, _type, _id, _timestamp, _source) select _index, _type, _id, _timestamp, _source FROM "
				+ indexTarget.getStagingTable());
	}
}
