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

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.elefana.api.document.DocumentShardInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.Timer;
import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkOpType;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.JsonException;

public class BulkTask implements Callable<List<BulkItemResponse>> {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkTask.class);

	private static final String FALLBACK_STAGING_TABLE_PREFIX = "elefana_fallback_stg_";
	private static int FALLBACK_STAGING_TABLE_ID = 0;

	private static final String DELIMITER_INIT = "e'\\x1f'";
	private static final String DELIMITER = Character.toString((char) 31);
	private static final String ESCAPE_INIT = "e'\\x1e'";
	private static final String ESCAPE = Character.toString((char) 30);
	private static final String NEW_LINE = "\n";
	private static final Charset CHARSET = Charset.forName("UTF-8");

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

	private static final long ONE_SECOND_IN_MILLIS = 1000L;
	private static final long ONE_MINUTE_IN_MILLIS = ONE_SECOND_IN_MILLIS * 60L;
	private static final long ONE_HOUR_IN_MILLIS = ONE_MINUTE_IN_MILLIS * 60L;
	private static final long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24L;

	private static final DocumentShardInfo SHARDS = new DocumentShardInfo();

	private final Timer psqlTimer;
	private final JdbcTemplate jdbcTemplate;
	private final List<BulkIndexOperation> indexOperations;
	private final String tablespace;
	private final String index;
	private final boolean flatten;
	private final int from;
	private final int size;

	private final String stagingTable;

	public BulkTask(Timer psqlTimer, JdbcTemplate jdbcTemplate, List<BulkIndexOperation> indexOperations,
			String tablespace, String index, boolean flatten, int from, int size) {
		super();
		this.psqlTimer = psqlTimer;
		this.jdbcTemplate = jdbcTemplate;
		this.indexOperations = indexOperations;
		this.tablespace = tablespace;
		this.index = index;
		this.flatten = flatten;
		this.from = from;
		this.size = size;

		List<Map<String, Object>> nextTableResults = jdbcTemplate
				.queryForList("SELECT elefana_next_bulk_ingest_table()");
		if (nextTableResults.isEmpty()) {
			LOGGER.error("Could not get next staging table ID from elefana_next_staging_table(), using fallback table");
			stagingTable = FALLBACK_STAGING_TABLE_PREFIX + FALLBACK_STAGING_TABLE_ID++;
		} else {
			Map<String, Object> row = nextTableResults.get(0);
			if (row.containsKey("table_name")) {
				stagingTable = ((String) row.get("table_name")).replace('-', '_');
			} else if (row.containsKey("elefana_next_bulk_ingest_table")) {
				stagingTable = ((String) row.get("elefana_next_bulk_ingest_table")).replace('-', '_');
			} else {
				LOGGER.error(
						"Could not get next staging table ID from elefana_next_staging_table(), using fallback table");
				stagingTable = FALLBACK_STAGING_TABLE_PREFIX + FALLBACK_STAGING_TABLE_ID++;
			}
		}
	}

	@Override
	public List<BulkItemResponse> call() {
		final List<BulkItemResponse> results = new ArrayList<BulkItemResponse>(1);
		Connection connection = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			StringBuilder createTableQuery = new StringBuilder();
			createTableQuery.append("CREATE TABLE ");
			createTableQuery.append(stagingTable);
			createTableQuery.append(" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, ");
			createTableQuery.append("_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb)");
			if (tablespace != null && !tablespace.isEmpty()) {
				createTableQuery.append(" TABLESPACE ");
				createTableQuery.append(tablespace);
			}

			PreparedStatement createTableStatement = connection.prepareStatement(createTableQuery.toString());
			createTableStatement.execute();
			createTableStatement.close();

			final PgConnection pgConnection = connection.unwrap(PgConnection.class);
			final CopyManager copyManager = new CopyManager(pgConnection);

			final Timer.Context timer = psqlTimer.time();

			final StringBuilder batchInsertQuery = new StringBuilder();
			batchInsertQuery.append("INSERT INTO ");
			batchInsertQuery.append(stagingTable);
			batchInsertQuery.append("(_index, _type, _id, _timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d, _source)");
			batchInsertQuery.append(" VALUES (?,?,?,?,?,?,?,?,?)");

			connection.setAutoCommit(false);
			PreparedStatement batchInsertStatement = connection.prepareStatement(batchInsertQuery.toString());
			try {
				for (int i = from; i < from + size && i < indexOperations.size(); i++) {
					BulkIndexOperation indexOperation = indexOperations.get(i);
					final long bucket1s = indexOperation.getTimestamp()
							- (indexOperation.getTimestamp() % ONE_SECOND_IN_MILLIS);
					final long bucket1m = indexOperation.getTimestamp()
							- (indexOperation.getTimestamp() % ONE_MINUTE_IN_MILLIS);
					final long bucket1h = indexOperation.getTimestamp()
							- (indexOperation.getTimestamp() % ONE_HOUR_IN_MILLIS);
					final long bucket1d = indexOperation.getTimestamp()
							- (indexOperation.getTimestamp() % ONE_DAY_IN_MILLIS);
					final String escapedJson;
					if(flatten) {
						escapedJson = IndexUtils.psqlEscapeString(IndexUtils.flattenJson(indexOperation.getSource()));
					} else {
						escapedJson = IndexUtils.psqlEscapeString(indexOperation.getSource());
					}

					batchInsertStatement.setString(1, indexOperation.getIndex());
					batchInsertStatement.setString(2, indexOperation.getType());
					batchInsertStatement.setString(3, indexOperation.getId());
					batchInsertStatement.setLong(4, indexOperation.getTimestamp());
					batchInsertStatement.setLong(5, bucket1s);
					batchInsertStatement.setLong(6, bucket1m);
					batchInsertStatement.setLong(7, bucket1h);
					batchInsertStatement.setLong(8, bucket1d);

					PGobject jsonObject = new PGobject();
					jsonObject.setType("json");
					jsonObject.setValue(escapedJson);

					batchInsertStatement.setObject(9, jsonObject);
					batchInsertStatement.addBatch();
				}
				batchInsertStatement.executeBatch();
				connection.commit();
				timer.stop();
			} catch (PSQLException e) {
				timer.stop();
				throw e;
			} catch (Exception e) {
				timer.stop();
				throw e;
			}

			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				BulkItemResponse responseEntry = createEntry(i, "index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId(), BulkItemResponse.STATUS_CREATED);
				results.add(responseEntry);
				indexOperation.release();
			}
		} catch (Exception e) {
			boolean foundBadEntry = false;
			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);

				if (!foundBadEntry) {
					try {
						JsonIterator.deserialize(indexOperation.getSource()).toString();
					} catch (JsonException ex) {
						LOGGER.error("Invalid JSON: " + indexOperation.getSource());
						foundBadEntry = true;
					}
				}

				BulkItemResponse responseEntry = null;
				if (foundBadEntry) {
					responseEntry = createEntry(i, "index", indexOperation.getIndex(), indexOperation.getType(),
							indexOperation.getId(), BulkItemResponse.STATUS_FAILED);
				} else {
					responseEntry = createEntry(i, "index", indexOperation.getIndex(), indexOperation.getType(),
							indexOperation.getId(), BulkItemResponse.STATUS_CREATED);
				}
				results.add(responseEntry);

				indexOperation.release();
			}
			if (!foundBadEntry) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		if (connection != null) {
			try {
				connection.setAutoCommit(true);
				connection.close();
			} catch (SQLException e) {
			}
		}
		return results;
	}

	public BulkItemResponse createEntry(int operationIndex, String operation, String index, String type, String id,
			String resultStatus) {
		BulkOpType opType = BulkOpType.valueOf(operation.toUpperCase());
		BulkItemResponse result = new BulkItemResponse(operationIndex, opType);
		result.setIndex(index);
		result.setType(type);
		result.setId(id);
		result.setVersion(1);
		result.setResult(resultStatus);
		return result;
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

	public String getStagingTable() {
		return stagingTable;
	}
}
