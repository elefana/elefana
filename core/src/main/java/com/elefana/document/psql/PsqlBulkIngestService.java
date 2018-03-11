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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import com.elefana.document.IndexTarget;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

@Service
public class PsqlBulkIngestService implements BulkIngestService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBulkIngestService.class);
	
	private static final String OPERATION_INDEX = "index";
	private static final String NEW_LINE = "\n";

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

	private String tablespace;
	private ExecutorService executorService;
	
	private Timer bulkOperationsTotalTimer, bulkOperationsPsqlTimer, bulkOperationsSerializationTimer;
	private Meter bulkOperationsSuccess, bulkOperationsFailed;
	
	@PostConstruct
	public void postConstruct() {
		tablespace = environment.getProperty("elefana.service.bulk.tablespace", "");

		final int totalThreads = environment.getProperty("elefana.service.bulk.threads", Integer.class, Runtime.getRuntime().availableProcessors());
		executorService = Executors.newFixedThreadPool(totalThreads);
		
		bulkOperationsTotalTimer = metricRegistry.timer(MetricRegistry.name("bulk", "operations", "duration", "total"));
		bulkOperationsSerializationTimer = metricRegistry.timer(MetricRegistry.name("bulk", "operations", "duration", "serialization"));
		bulkOperationsPsqlTimer = metricRegistry.timer(MetricRegistry.name("bulk", "operations", "duration", "psql"));
		bulkOperationsSuccess = metricRegistry.meter(MetricRegistry.name("bulk", "operations", "success"));
		bulkOperationsFailed = metricRegistry.meter(MetricRegistry.name("bulk", "operations", "failed"));
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
	}
	
	@Override
	public BulkRequest prepareBulkRequest(String requestBody) {
		return new PsqlBulkRequest(this, requestBody);
	}

	public BulkResponse bulkOperations(String requestBody) throws ElefanaException {
		final Timer.Context totalTimer = bulkOperationsTotalTimer.time();
		
		final BulkResponse bulkApiResponse = new BulkResponse();

		final String[] lines = requestBody.split(NEW_LINE);
		
		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

		final Timer.Context serializationTimer = bulkOperationsSerializationTimer.time();
		try {
			for (int i = 0; i < lines.length; i += 2) {
				if(i + 1 >= lines.length) {
					break;
				}
				Any operation = JsonIterator.deserialize(lines[i]);
				if (!operation.get(OPERATION_INDEX).valueType().equals(ValueType.INVALID)) {
					Any indexOperationTarget = operation.get(OPERATION_INDEX);

					BulkIndexOperation indexOperation = BulkIndexOperation.allocate();
					indexOperation.setIndex(indexOperationTarget.get(BulkTask.KEY_INDEX).toString());
					indexOperation.setType(indexOperationTarget.get(BulkTask.KEY_TYPE).toString());
					if (!indexOperationTarget.get(BulkTask.KEY_ID).valueType().equals(ValueType.INVALID)) {
						indexOperation.setId(indexOperationTarget.get(BulkTask.KEY_ID).toString());
					} else {
						indexOperation.setId(UUID.randomUUID().toString());
					}
					indexOperation.setSource(lines[i + 1]);
					indexOperation.setTimestamp(indexUtils.getTimestamp(indexOperation.getIndex(), indexOperation.getSource()));
					
					if (!indexOperations.containsKey(indexOperation.getIndex())) {
						indexOperations.put(indexOperation.getIndex(), new ArrayList<BulkIndexOperation>(1));
					}
					indexOperations.get(indexOperation.getIndex()).add(indexOperation);
				}
				// TODO: Handle other operations
			}
		} finally {
			serializationTimer.stop();
		}

		bulkApiResponse.setErrors(false);
		for (String index : indexOperations.keySet()) {
			bulkIndex(bulkApiResponse, index, indexOperations.get(index));
		}
		
		final long duration = totalTimer.stop();
		bulkApiResponse.setTook(TimeUnit.NANOSECONDS.toMillis(duration));
		return bulkApiResponse;
	}

	private void bulkIndex(BulkResponse bulkApiResponse, String index,
			List<BulkIndexOperation> indexOperations) throws ElefanaException {
		indexUtils.ensureIndexExists(index);
		final String queryTarget = indexUtils.getQueryTarget(index);
		
		final int operationSize = Math.max(250, indexOperations.size() / nodeSettingsService.getBulkParallelisation());
		
		final List<BulkTask> bulkTasks = new ArrayList<BulkTask>();
		final List<Future<List<BulkItemResponse>>> results = new ArrayList<Future<List<BulkItemResponse>>>();
		
		for(int i = 0; i < indexOperations.size(); i += operationSize) {
			final BulkTask task = new BulkTask(bulkOperationsPsqlTimer, jdbcTemplate, indexOperations, tablespace, index, queryTarget, i, operationSize);
			bulkTasks.add(task);
			results.add(executorService.submit(task));
		}
		
		for(int i = 0; i < results.size(); i++) {
			final BulkTask task = bulkTasks.get(i);
			try {
				List<BulkItemResponse> nextResult = results.get(i).get();
				if(nextResult.isEmpty()) {
					bulkOperationsFailed.mark(task.getSize());
					bulkApiResponse.setErrors(true);
				} else {
					bulkOperationsSuccess.mark(task.getSize());
					bulkApiResponse.getItems().addAll(nextResult);
				}
				bulkIndexService.queue(new IndexTarget(index, queryTarget, task.getStagingTable()));
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
