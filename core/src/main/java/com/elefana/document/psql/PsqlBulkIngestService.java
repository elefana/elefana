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

import java.sql.Connection;
import java.sql.PreparedStatement;
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

import com.elefana.api.json.V2BulkResponseEncoder;
import com.elefana.api.json.V5BulkResponseEncoder;
import com.elefana.node.VersionInfoService;
import com.jsoniter.spi.JsoniterSpi;
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
	protected VersionInfoService versionInfoService;
	@Autowired
	private IngestTableTracker ingestTableTracker;
	@Autowired
	private MetricRegistry metricRegistry;

	private final AtomicInteger tablespaceIndex = new AtomicInteger();
	private ExecutorService bulkRequestExecutorService, bulkProcessingExecutorService;

	private Timer bulkIngestTotalTimer, bulkIngestPsqlTimer, bulkIngestSerializationTimer, bulkGatherResultsTimer;
	private Timer psqlBatchBuildTimer, jsonFlattenTimer, jsonEscapeTimer;
	private Meter bulkOperationsSuccess, bulkOperationsFailed, bulkOperationsBatchSize;

	@PostConstruct
	public void postConstruct() {
		final int totalIngestThreads = environment.getProperty("elefana.service.bulk.ingest.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalProcessingThreads = (nodeSettingsService.getBulkParallelisation() * totalIngestThreads) + 1;
		bulkRequestExecutorService = Executors.newFixedThreadPool(totalIngestThreads, new NamedThreadFactory(REQUEST_THREAD_PREFIX));
		bulkProcessingExecutorService = Executors.newFixedThreadPool(totalProcessingThreads, new NamedThreadFactory(PROCESSOR_THREAD_PREFIX));

		bulkIngestTotalTimer = metricRegistry.timer(MetricRegistry.name("bulk", "ingest", "duration", "total"));
		bulkIngestSerializationTimer = metricRegistry
				.timer(MetricRegistry.name("bulk", "ingest", "duration", "serialization"));
		bulkIngestPsqlTimer = metricRegistry.timer(MetricRegistry.name("bulk", "ingest", "duration", "psql"));
		bulkGatherResultsTimer = metricRegistry.timer(MetricRegistry.name("bulk", "ingest", "duration", "gather"));

		bulkOperationsBatchSize = metricRegistry.meter(MetricRegistry.name("bulk", "ingest", "batch", "size"));
		bulkOperationsSuccess = metricRegistry.meter(MetricRegistry.name("bulk", "ingest", "success"));
		bulkOperationsFailed = metricRegistry.meter(MetricRegistry.name("bulk", "ingest", "failed"));

		psqlBatchBuildTimer = metricRegistry.timer(MetricRegistry.name("bulk", "ingest", "duration", "batch"));
		jsonFlattenTimer = metricRegistry.timer(MetricRegistry.name("json",  "duration", "flatten"));
		jsonEscapeTimer = metricRegistry.timer(MetricRegistry.name("json", "duration", "escape"));

		switch(versionInfoService.getApiVersion()) {
		case V_5_5_2:
			JsoniterSpi.registerTypeEncoder(BulkResponse.class, new V2BulkResponseEncoder());
			break;
		default:
		case V_2_4_3:
			JsoniterSpi.registerTypeEncoder(BulkResponse.class, new V5BulkResponseEncoder());
			break;
		}
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
		final Timer.Context totalTimer = bulkIngestTotalTimer.time();

		final String[] lines = requestBody.split(NEW_LINE);
		int responseCapacity = lines.length / 2;
		bulkOperationsBatchSize.mark(responseCapacity);

		final BulkResponse bulkApiResponse = new BulkResponse(responseCapacity + 1);
		bulkApiResponse.setErrors(false);

		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

		final Timer.Context serializationTimer = bulkIngestSerializationTimer.time();
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
						indexOperation.setSource(sourceLine);

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
		final IngestTable ingestTable = ingestTableTracker.getIngestTable(index);

		final int operationSize = Math.max(MINIMUM_BULK_SIZE, indexOperations.size() / nodeSettingsService.getBulkParallelisation());
		
		final List<BulkTask> bulkTasks = new ArrayList<BulkTask>();
		List<Future<List<BulkItemResponse>>> results = null;

		for (int i = 0; i < indexOperations.size(); i += operationSize) {
			final BulkTask task = new BulkTask(jdbcTemplate, indexOperations,
					index, ingestTable, nodeSettingsService.isFlattenJson(), i, operationSize,
					bulkIngestPsqlTimer, psqlBatchBuildTimer, jsonFlattenTimer, jsonEscapeTimer);
			bulkTasks.add(task);
		}
		try {
			results = bulkProcessingExecutorService.invokeAll(bulkTasks);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}

		final Timer.Context gatherTimer = bulkGatherResultsTimer.time();

		boolean success = true;
		for (int i = 0; i < results.size(); i++) {
			final BulkTask task = bulkTasks.get(i);
			try {
				final List<BulkItemResponse> nextResult = results.get(i).get();
				if (nextResult.isEmpty()) {
					bulkOperationsFailed.mark(task.getSize());
					success = false;
				} else {
					bulkOperationsSuccess.mark(task.getTotalSuccess());
					bulkOperationsFailed.mark(task.getTotalFailed());

					if(task.getTotalFailed() > 0) {
						success = false;
					}
				}
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		gatherTimer.stop();

		if(!success) {
			bulkApiResponse.setErrors(true);
		}
		for (int i = 0; i < results.size(); i++) {
			try {
				final List<BulkItemResponse> nextResult = results.get(i).get();

				if(!success) {
					for(BulkItemResponse response : nextResult) {
						response.setResult(BulkItemResponse.STATUS_FAILED);
					}
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
}
