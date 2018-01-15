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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.util.IndexUtils;

public class BulkTask implements Callable<List<Map<String, Object>>> {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkTask.class);
	
	private static final String DELIMITER = "|";
	private static final String NEW_LINE = "\n";
	
	public static final String KEY_INDEX = "_index";
	public static final String KEY_TYPE = "_type";
	public static final String KEY_ID = "_id";
	public static final String KEY_VERSION = "_version";
	public static final String KEY_SHARDS = "_shards";
	
	public static final String KEY_RESULT = "result";
	public static final String VALUE_RESULT_CREATED = "created";
	public static final String KEY_CREATED = "created";
	public static final boolean VALUE_CREATED = true;
	public static final String KEY_STATUS = "status";
	public static final int VALUE_STATUS_CREATED = 201;
	
	private static final Map<String, Object> SHARDS = new HashMap<String, Object>() {
		{
			put("total", 1);
			put("successful", 1);
			put("failed", 0);
		}
	};
	
	private final JdbcTemplate jdbcTemplate;
	private final List<BulkIndexOperation> indexOperations;
	private final String index;
	private final int from;
	private final int size;

	public BulkTask(JdbcTemplate jdbcTemplate, List<BulkIndexOperation> indexOperations, String index, int from, int size) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.indexOperations = indexOperations;
		this.index = index;
		this.from = from;
		this.size = size;
	}

	@Override
	public List<Map<String, Object>> call() throws Exception {
		final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>(1);
		Connection connection = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			final PgConnection pgConnection = connection.unwrap(PgConnection.class);
			final CopyManager copyManager = new CopyManager(pgConnection);

			CopyIn copyIn = copyManager
					.copyIn("COPY " + IndexUtils.DATA_TABLE + " FROM STDIN WITH DELIMITER '" + DELIMITER + "'");

			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				final String row = indexOperation.getIndex() + DELIMITER + indexOperation.getType() + DELIMITER
						+ indexOperation.getId() + DELIMITER + System.currentTimeMillis() + DELIMITER  + indexOperation.getSource() + NEW_LINE;
				final byte [] rowBytes = row.getBytes();
				copyIn.writeToCopy(rowBytes, 0, rowBytes.length);
			}
			copyIn.endCopy();
			
			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				Map<String, Object> responseEntry = createEntry(results, "index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId());
				responseEntry.put(KEY_RESULT, VALUE_RESULT_CREATED);
				responseEntry.put(KEY_CREATED, VALUE_CREATED);
				responseEntry.put(KEY_STATUS, VALUE_STATUS_CREATED);
				indexOperation.release();
			}
		} catch (Exception e) {
			for (BulkIndexOperation indexOperation : indexOperations) {
				if(e.getMessage().contains(indexOperation.getId())) {
					LOGGER.info(indexOperation.getSource());
				}
			}
			LOGGER.error(e.getMessage(), e);
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
		return results;
	}

	public Map<String, Object> createEntry(List<Map<String, Object>> results, String operation, String index, String type, String id) {
		final Map<String, Object> entry = new HashMap<String, Object>();
		final Map<String, Object> entryData = new HashMap<String, Object>();
		
		entryData.put(KEY_INDEX, index);
		entryData.put(KEY_TYPE, type);
		entryData.put(KEY_ID, id);
		entryData.put(KEY_VERSION, 1);
		entryData.put(KEY_SHARDS, SHARDS);
		
		entry.put(operation, entryData);
		results.add(entry);
		return entryData;
	}

	public String getIndex() {
		return index;
	}

	public int getFrom() {
		return from;
	}

	public int getSize() {
		return size;
	}
}
