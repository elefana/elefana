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
package com.elefana.document;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

@Service
public class BulkService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkService.class);
	
	private static final String OPERATION_INDEX = "index";
	private static final String NEW_LINE = "\n";

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;
	@Autowired
	private ScheduledExecutorService scheduledExecutorService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	public BulkApiResponse bulkOperations(String requestBody) throws Exception {
		final long startTime = System.currentTimeMillis();
		final BulkApiResponse bulkApiResponse = new BulkApiResponse();

		final String[] lines = requestBody.split(NEW_LINE);

		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

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

				if (!indexOperations.containsKey(indexOperation.getIndex())) {
					indexOperations.put(indexOperation.getIndex(), new ArrayList<BulkIndexOperation>(1));
				}
				indexOperations.get(indexOperation.getIndex()).add(indexOperation);
			}
			// TODO: Handle other operations
		}

		bulkApiResponse.setErrors(false);
		for (String index : indexOperations.keySet()) {
			bulkIndex(bulkApiResponse, index, indexOperations.get(index));
		}
		bulkApiResponse.setTook(System.currentTimeMillis() - startTime);
		return bulkApiResponse;
	}

	private void bulkIndex(BulkApiResponse bulkApiResponse, String index,
			List<BulkIndexOperation> indexOperations) throws SQLException {
		indexUtils.ensureIndexExists(index);
		
		final int operationSize = Math.max(1000, indexOperations.size() / nodeSettingsService.getBulkParallelisation());
		final List<Future<List<Map<String, Object>>>> results = new ArrayList<Future<List<Map<String, Object>>>>();
		
		long pgStartTime = System.currentTimeMillis();
		for(int i = 0; i < indexOperations.size(); i += operationSize) {
			results.add(scheduledExecutorService.submit(new BulkTask(jdbcTemplate, indexOperations, index, i, operationSize)));
		}
		
		for(int i = 0; i < results.size(); i++) {
			try {
				List<Map<String, Object>> nextResult = results.get(i).get();
				if(nextResult.isEmpty()) {
					bulkApiResponse.setErrors(true);
				} else {
					bulkApiResponse.getItems().addAll(nextResult);
				}
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		indexFieldMappingService.scheduleIndexForMappingAndStats(index);
		LOGGER.info("Indexed " + indexOperations.size() + " into index '" + index + "' in " + (System.currentTimeMillis() - pgStartTime) + "ms");
	}
}
