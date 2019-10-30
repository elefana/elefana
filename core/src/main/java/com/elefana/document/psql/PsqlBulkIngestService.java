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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.elefana.api.RequestExecutor;
import com.elefana.api.document.*;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.IndexTimeBucket;
import com.elefana.api.json.V2BulkResponseEncoder;
import com.elefana.api.json.V5BulkResponseEncoder;
import com.elefana.document.*;
import com.elefana.document.ingest.HashIngestTable;
import com.elefana.document.ingest.IngestTableTracker;
import com.elefana.document.ingest.TimeIngestTable;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.JsonException;
import com.jsoniter.spi.JsoniterSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
	private IndexTemplateService indexTemplateService;
	@Autowired
	private MetricRegistry metricRegistry;
	@Autowired
	private IndexFieldStatsService fieldStatsService;

	private final AtomicInteger tablespaceIndex = new AtomicInteger();
	private ExecutorService bulkRequestExecutorService, bulkProcessingExecutorService;

	private Timer bulkIndexTimer;
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

		bulkIndexTimer = metricRegistry.timer(MetricRegistry.name("bulk", "index", "duration", "total"));

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

		try {
			bulkRequestExecutorService.awaitTermination(120, TimeUnit.SECONDS);
			bulkProcessingExecutorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}

	@Override
	public BulkRequest prepareBulkRequest(String requestBody) {
		return new PsqlBulkRequest(this, requestBody);
	}

	@Override
	public BulkStatsRequest prepareBulkStatsRequest() {
		return new PsqlBulkStatsRequest(this);
	}

	public BulkStatsResponse getBulkStats() {
		final Map<String, Object> durationStats = new LinkedHashMap<String, Object>();
		final Snapshot durationSnapshot = bulkIngestTotalTimer.getSnapshot();
		durationStats.put("p99", TimeUnit.NANOSECONDS.toMillis((long) durationSnapshot.get99thPercentile()) + "ms");
		durationStats.put("p95", TimeUnit.NANOSECONDS.toMillis((long) durationSnapshot.get95thPercentile()) + "ms");
		durationStats.put("p75", TimeUnit.NANOSECONDS.toMillis((long) durationSnapshot.get75thPercentile()) + "ms");
		
		final Map<String, Object> successStats = new LinkedHashMap<String, Object>();
		successStats.put("m1", bulkOperationsSuccess.getOneMinuteRate());
		successStats.put("m5", bulkOperationsSuccess.getFiveMinuteRate());
		successStats.put("m15", bulkOperationsSuccess.getFifteenMinuteRate());
		successStats.put("total", bulkOperationsSuccess.getCount());

		final Map<String, Object> failureStats = new LinkedHashMap<String, Object>();
		failureStats.put("m1", bulkOperationsFailed.getOneMinuteRate());
		failureStats.put("m5", bulkOperationsFailed.getFiveMinuteRate());
		failureStats.put("m15", bulkOperationsFailed.getFifteenMinuteRate());
		failureStats.put("total", bulkOperationsFailed.getCount());

		final Map<String, Object> batchStats = new LinkedHashMap<>();
		batchStats.put("m1", bulkOperationsBatchSize.getOneMinuteRate());
		batchStats.put("m5", bulkOperationsBatchSize.getFiveMinuteRate());
		batchStats.put("m15", bulkOperationsBatchSize.getFifteenMinuteRate());
		batchStats.put("total", bulkOperationsBatchSize.getCount());

		final Map<String, Object> indexStats = new LinkedHashMap<String, Object>();
		final Snapshot indexSnapshot = bulkIndexTimer.getSnapshot();
		indexStats.put("p99", TimeUnit.NANOSECONDS.toMillis((long) indexSnapshot.get99thPercentile()) + "ms");
		indexStats.put("p95", TimeUnit.NANOSECONDS.toMillis((long) indexSnapshot.get95thPercentile()) + "ms");
		indexStats.put("p75", TimeUnit.NANOSECONDS.toMillis((long) indexSnapshot.get75thPercentile()) + "ms");

		final BulkStatsResponse response = new BulkStatsResponse();
		response.getStats().put("duration", durationStats);
		response.getStats().put("success", successStats);
		response.getStats().put("failure", failureStats);
		response.getStats().put("batch", batchStats);
		response.getStats().put("index", indexStats);
		response.getStats().put("tables", ingestTableTracker.getTotalIngestTables());
		return response;
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
						indexOperation.setIndex(indexOperationTarget.get(BulkHashIndexTask.KEY_INDEX).toString());
						indexOperation.setType(indexOperationTarget.get(BulkHashIndexTask.KEY_TYPE).toString());
						indexOperation.setSource(sourceLine);

						indexOperation.setTimestamp(
								indexUtils.getTimestamp(indexOperation.getIndex(), indexOperation.getSource()));

						if (!indexOperationTarget.get(BulkHashIndexTask.KEY_ID).valueType().equals(ValueType.INVALID)) {
							indexOperation.setId(indexOperationTarget.get(BulkHashIndexTask.KEY_ID).toString());
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
			indexUtils.ensureIndexExists(index);

			if(nodeSettingsService.isUsingCitus()) {
				final IndexTemplate indexTemplate;
				if(indexTemplateService instanceof PsqlIndexTemplateService) {
					indexTemplate = ((PsqlIndexTemplateService) indexTemplateService).getIndexTemplateForIndex(index);
				} else {
					indexTemplate = indexTemplateService.prepareGetIndexTemplateForIndex(index).get().getIndexTemplate();
				}
				if(indexTemplate != null && indexTemplate.isTimeSeries()) {
					bulkIndexTime(bulkApiResponse, indexTemplate, index, indexOperations.get(index));
				} else {
					bulkIndexHash(bulkApiResponse, index, indexOperations.get(index));
				}
			} else {
				bulkIndexHash(bulkApiResponse, index, indexOperations.get(index));
			}
		}

		final long duration = totalTimer.stop();
		bulkApiResponse.setTook(TimeUnit.NANOSECONDS.toMillis(duration));
		return bulkApiResponse;
	}

	private void bulkIndexTime(BulkResponse bulkApiResponse, IndexTemplate indexTemplate, String index, List<BulkIndexOperation> indexOperations)
			throws ElefanaException {
		final IndexTimeBucket indexTimeBucket = indexTemplate.getStorage().getIndexTimeBucket();
		final TimeIngestTable timeIngestTable = ingestTableTracker.getTimeIngestTable(index);
		final int operationSize = Math.max(MINIMUM_BULK_SIZE, indexOperations.size() / nodeSettingsService.getBulkParallelisation());

		final Map<Integer, List<BulkIndexOperation>> operationsByShard = new HashMap<Integer, List<BulkIndexOperation>>();
		for(BulkIndexOperation bulkIndexOperation : indexOperations) {
			final int shard = indexTimeBucket.getShardOffset(bulkIndexOperation.getTimestamp());
			if(!operationsByShard.containsKey(shard)) {
				operationsByShard.put(shard, new ArrayList<BulkIndexOperation>());
			}
			operationsByShard.get(shard).add(bulkIndexOperation);
		}

		final List<BulkIndexTask> bulkTimeIndexTasks = new ArrayList<BulkIndexTask>();
		for(int shard : operationsByShard.keySet()) {
			final List<BulkIndexOperation> bulkIndexOperations = operationsByShard.get(shard);
			for (int i = 0; i < bulkIndexOperations.size(); i += operationSize) {
				final BulkTimeIndexTask task = new BulkTimeIndexTask(jdbcTemplate, bulkIndexOperations,
						index, timeIngestTable, nodeSettingsService.isFlattenJson(), i, operationSize, shard,
						bulkIngestPsqlTimer, psqlBatchBuildTimer, jsonFlattenTimer, jsonEscapeTimer, fieldStatsService);
				bulkTimeIndexTasks.add(task);
			}
		}

		bulkItemResponse(bulkApiResponse, index, bulkTimeIndexTasks);
	}

	private void bulkIndexHash(BulkResponse bulkApiResponse, String index, List<BulkIndexOperation> indexOperations)
			throws ElefanaException {
		final HashIngestTable hashIngestTable = ingestTableTracker.getHashIngestTable(index);
		final int operationSize = Math.max(MINIMUM_BULK_SIZE, indexOperations.size() / nodeSettingsService.getBulkParallelisation());
		
		final List<BulkIndexTask> bulkHashIndexTasks = new ArrayList<BulkIndexTask>();

		for (int i = 0; i < indexOperations.size(); i += operationSize) {
			final BulkHashIndexTask task = new BulkHashIndexTask(jdbcTemplate, indexOperations,
					index, hashIngestTable, nodeSettingsService.isFlattenJson(), i, operationSize,
					bulkIngestPsqlTimer, psqlBatchBuildTimer, jsonFlattenTimer, jsonEscapeTimer, fieldStatsService);
			bulkHashIndexTasks.add(task);
		}
		bulkItemResponse(bulkApiResponse, index, bulkHashIndexTasks);
	}

	private void bulkItemResponse(BulkResponse bulkApiResponse, String index, List<BulkIndexTask> bulkIndexTasks ) {
		List<Future<List<BulkItemResponse>>> results = null;
		try {
			results = bulkProcessingExecutorService.invokeAll(bulkIndexTasks);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}

		final Timer.Context gatherTimer = bulkGatherResultsTimer.time();

		boolean success = true;
		for (int i = 0; i < results.size(); i++) {
			final BulkIndexTask task = bulkIndexTasks.get(i);
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
