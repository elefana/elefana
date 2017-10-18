/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.document;

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

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.viridiansoftware.es2pgsql.util.TableUtils;

@Service
public class BulkService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkService.class);
	
	private static final String OPERATION_INDEX = "index";
	private static final String NEW_LINE = "\n";
	private static final int BULK_PARALLELISATION = Runtime.getRuntime().availableProcessors() * 2;

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;
	@Autowired
	private ScheduledExecutorService scheduledExecutorService;

	public BulkApiResponse bulkOperations(String requestBody) throws Exception {
		final long startTime = System.currentTimeMillis();
		final BulkApiResponse bulkApiResponse = new BulkApiResponse();

		final String[] lines = requestBody.split(NEW_LINE);

		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

		for (int i = 0; i < lines.length; i += 2) {
			Any operation = JsonIterator.deserialize(lines[i]);
			if (!operation.get(OPERATION_INDEX).valueType().equals(ValueType.INVALID)) {
				Any indexOperationTarget = operation.get(OPERATION_INDEX);

				BulkIndexOperation indexOperation = BulkIndexOperation.allocate();
				indexOperation.setIndex(indexOperationTarget.get(BulkTask.KEY_INDEX).toString());
				indexOperation.setType(indexOperationTarget.get(BulkTask.KEY_TYPE).toString());
				if (!indexOperationTarget.get(BulkTask.KEY_ID).equals(ValueType.INVALID)) {
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

		for (String index : indexOperations.keySet()) {
			bulkIndex(bulkApiResponse, index, indexOperations.get(index));
		}
		bulkApiResponse.setTook(System.currentTimeMillis() - startTime);
		bulkApiResponse.setErrors(false);
		return bulkApiResponse;
	}

	private void bulkIndex(BulkApiResponse bulkApiResponse, String index,
			List<BulkIndexOperation> indexOperations) throws SQLException {
		tableUtils.ensureTableExists(index);
		
		final int operationSize = Math.max(1000, indexOperations.size() / BULK_PARALLELISATION);
		final List<Future<List<Map<String, Object>>>> results = new ArrayList<Future<List<Map<String, Object>>>>();
		
		long pgStartTime = System.currentTimeMillis();
		for(int i = 0; i < indexOperations.size(); i += operationSize) {
			results.add(scheduledExecutorService.submit(new BulkTask(jdbcTemplate, indexOperations, index, i, operationSize)));
		}
		
		for(int i = 0; i < results.size(); i++) {
			try {
				bulkApiResponse.getItems().addAll(results.get(i).get());
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (ExecutionException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		LOGGER.info("Indexed " + indexOperations.size() + " into index '" + index + "' in " + (System.currentTimeMillis() - pgStartTime) + "ms");
	}
}
