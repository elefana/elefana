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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.elefana.api.RequestExecutor;
import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkRequest;
import com.elefana.api.document.BulkResponse;
import com.elefana.api.exception.ElefanaException;
import com.elefana.document.BulkIndexOperation;
import com.elefana.document.BulkIngestService;
import com.elefana.document.BulkTask;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.JsonException;

@Service
public class PsqlBulkIngestService implements BulkIngestService, RequestExecutor {
	private static final String[] DEFAULT_TABLESPACES = new String[] { "" };
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBulkIngestService.class);
	private static final String THREAD_PREFIX = BulkIngestService.class.getSimpleName() + "-";
	private static final String REQUEST_THREAD_PREFIX = THREAD_PREFIX + "requestHandler" + "-";
	private static final String PROCESSOR_THREAD_PREFIX = THREAD_PREFIX + "processor" + "-";

	private static final String OPERATION_INDEX = "index";
	private static final String NEW_LINE = "\\}(\\s)*\n";

	public static final int MINIMUM_BULK_SIZE = 250;
	
	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private PsqlBulkIndexService bulkIndexService;
	@Autowired
	private MetricRegistry metricRegistry;

	private final AtomicInteger tablespaceIndex = new AtomicInteger();
	private String[] tablespaces;
	private ExecutorService bulkRequestExecutorService, bulkProcessingExecutorService;

	private Timer bulkOperationsTotalTimer, bulkOperationsPsqlTimer, bulkOperationsSerializationTimer;
	private Meter bulkOperationsSuccess, bulkOperationsFailed;

	@PostConstruct
	public void postConstruct() {
		tablespaces = environment.getProperty("elefana.service.bulk.tablespaces", "").split(",");
		if (isEmptyTablespaceList(tablespaces)) {
			tablespaces = DEFAULT_TABLESPACES;
		}

		final int totalThreads = environment.getProperty("elefana.service.bulk.ingest.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		bulkRequestExecutorService = Executors.newFixedThreadPool(totalThreads, new NamedThreadFactory(REQUEST_THREAD_PREFIX));
		bulkProcessingExecutorService = Executors.newFixedThreadPool(totalThreads, new NamedThreadFactory(PROCESSOR_THREAD_PREFIX));

		bulkOperationsTotalTimer = metricRegistry.timer(MetricRegistry.name("bulk", "operations", "duration", "total"));
		bulkOperationsSerializationTimer = metricRegistry
				.timer(MetricRegistry.name("bulk", "operations", "duration", "serialization"));
		bulkOperationsPsqlTimer = metricRegistry.timer(MetricRegistry.name("bulk", "operations", "duration", "psql"));
		bulkOperationsSuccess = metricRegistry.meter(MetricRegistry.name("bulk", "operations", "success"));
		bulkOperationsFailed = metricRegistry.meter(MetricRegistry.name("bulk", "operations", "failed"));
	}

	@PreDestroy
	public void preDestroy() {
		bulkRequestExecutorService.shutdown();
		bulkProcessingExecutorService.shutdown();
	}

	@Override
	public BulkRequest prepareBulkRequest(String requestBody) {
		return new PsqlBulkRequest(this, requestBody);
	}

	public BulkResponse bulkOperations(String requestBody) throws ElefanaException {
		final Timer.Context totalTimer = bulkOperationsTotalTimer.time();

		final BulkResponse bulkApiResponse = new BulkResponse();
		bulkApiResponse.setErrors(false);

		final String[] lines = requestBody.split(NEW_LINE);

		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

		final Timer.Context serializationTimer = bulkOperationsSerializationTimer.time();
		try {
			for (int i = 0; i < lines.length; i += 2) {
				if (i + 1 >= lines.length) {
					break;
				}
				try {
					final String line = lines[i] + "}";
					Any operation = JsonIterator.deserialize(line);
					if (!operation.get(OPERATION_INDEX).valueType().equals(ValueType.INVALID)) {
						final String sourceLine = lines[i + 1] + "}";
						Any indexOperationTarget = operation.get(OPERATION_INDEX);

						BulkIndexOperation indexOperation = BulkIndexOperation.allocate();
						indexOperation.setIndex(indexOperationTarget.get(BulkTask.KEY_INDEX).toString());
						indexOperation.setType(indexOperationTarget.get(BulkTask.KEY_TYPE).toString());

						if(nodeSettingsService.isFlattenJson()) {
							indexOperation.setSource(IndexUtils.flattenJson(sourceLine));
						} else {
							indexOperation.setSource(sourceLine);
						}

						indexOperation.setTimestamp(
								indexUtils.getTimestamp(indexOperation.getIndex(), indexOperation.getSource()));

						if (!indexOperationTarget.get(BulkTask.KEY_ID).valueType().equals(ValueType.INVALID)) {
							indexOperation.setId(indexOperationTarget.get(BulkTask.KEY_ID).toString());
						} else {
							indexOperation.setId(indexUtils.generateDocumentId(indexOperation.getIndex(),
									indexOperation.getType(), indexOperation.getSource()));
						}

						if (!indexOperations.containsKey(indexOperation.getIndex())) {
							indexOperations.put(indexOperation.getIndex(), new ArrayList<BulkIndexOperation>(1));
						}
						indexOperations.get(indexOperation.getIndex()).add(indexOperation);
					} else {
						bulkApiResponse.setErrors(true);
						LOGGER.error("Invalid JSON at line number " + (i + 1) + ": " + line);
						break;
					}
					// TODO: Handle other operations
				} catch (JsonException e) {
					LOGGER.error("Error parsing JSON at line number " + (i + 1) + ": " + lines[i] + "} - " + e.getMessage(), e);
					bulkApiResponse.setErrors(true);
				} catch (IOException e) {
					LOGGER.error("Error parsing JSON at line number " + (i + 1) + ": " + lines[i] + "} - " + e.getMessage(), e);
					bulkApiResponse.setErrors(true);
				}
			}
		} finally {
			serializationTimer.stop();
		}

		for (String index : indexOperations.keySet()) {
			bulkIndex(bulkApiResponse, index, indexOperations.get(index));
		}

		final long duration = totalTimer.stop();
		bulkApiResponse.setTook(TimeUnit.NANOSECONDS.toMillis(duration));
		return bulkApiResponse;
	}

	private void bulkIndex(BulkResponse bulkApiResponse, String index, List<BulkIndexOperation> indexOperations)
			throws ElefanaException {
		indexUtils.ensureIndexExists(index);
		final String queryTarget = indexUtils.getQueryTarget(index);

		final int operationSize = Math.max(MINIMUM_BULK_SIZE, indexOperations.size() / nodeSettingsService.getBulkParallelisation());
		
		final List<BulkTask> bulkTasks = new ArrayList<BulkTask>();
		final List<Future<List<BulkItemResponse>>> results = new ArrayList<Future<List<BulkItemResponse>>>();

		for (int i = 0; i < indexOperations.size(); i += operationSize) {
			final String tablespace = tablespaces[tablespaceIndex.incrementAndGet() % tablespaces.length];
			final BulkTask task = new BulkTask(bulkOperationsPsqlTimer, jdbcTemplate, indexOperations, tablespace,
					index, queryTarget, i, operationSize);
			bulkTasks.add(task);
			try {
				results.add(bulkProcessingExecutorService.submit(task));
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		boolean success = true;
		for (int i = 0; i < results.size(); i++) {
			final BulkTask task = bulkTasks.get(i);
			try {
				final List<BulkItemResponse> nextResult = results.get(i).get();
				if (nextResult.isEmpty()) {
					bulkOperationsFailed.mark(task.getSize());
					success = false;
				} else {
					for(int j = 0; j < nextResult.size(); j++) {
						BulkItemResponse response = nextResult.get(j);
						if(response.isFailed()) {
							success = false;
							bulkOperationsFailed.mark();
						} else {
							bulkOperationsSuccess.mark();
						}
					}
				}
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		if(!success) {
			bulkApiResponse.setErrors(true);
		}

		for (int i = 0; i < results.size(); i++) {
			try {
				final BulkTask task = bulkTasks.get(i);
				final List<BulkItemResponse> nextResult = results.get(i).get();

				if(success) {
					jdbcTemplate.execute("INSERT INTO elefana_bulk_index_queue (_tableName, _queue_id) VALUES ('" + task.getStagingTable() + "', nextval('elefana_bulk_index_queue_id'))");
				} else {
					for(BulkItemResponse response : nextResult) {
						response.setResult(BulkItemResponse.STATUS_FAILED);
					}

					jdbcTemplate.execute("DELETE FROM " + task.getStagingTable());
				}

				bulkApiResponse.getItems().addAll(nextResult);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return bulkRequestExecutorService.submit(request);
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
