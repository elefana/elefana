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

	private static final long ONE_SECOND_IN_MILLIS = 1000L;
	private static final long ONE_MINUTE_IN_MILLIS = ONE_SECOND_IN_MILLIS * 60L;
	private static final long ONE_HOUR_IN_MILLIS = ONE_MINUTE_IN_MILLIS * 60L;
	private static final long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24L;

	private static final DocumentShardInfo SHARDS = new DocumentShardInfo();

	private final Timer psqlTimer, batchBuildTimer, flattenTimer, escapeTimer;
	private final JdbcTemplate jdbcTemplate;
	private final List<BulkIndexOperation> indexOperations;
	private final String tablespace;
	private final String index;
	private final boolean flatten;
	private final int from;
	private final int size;

	private String stagingTable;
	private volatile int totalFailed, totalSuccess;

	public BulkTask(JdbcTemplate jdbcTemplate, List<BulkIndexOperation> indexOperations,
			String tablespace, String index, boolean flatten, int from, int size,
			        Timer psqlTimer, Timer batchBuildTimer, Timer flattenTimer, Timer escapeTimer) {
		super();
		this.psqlTimer = psqlTimer;
		this.batchBuildTimer = batchBuildTimer;
		this.flattenTimer = flattenTimer;
		this.escapeTimer = escapeTimer;

		this.jdbcTemplate = jdbcTemplate;
		this.indexOperations = indexOperations;
		this.tablespace = tablespace;
		this.index = index;
		this.flatten = flatten;
		this.from = from;
		this.size = size;
	}

	@Override
	public List<BulkItemResponse> call() {
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

		final List<BulkItemResponse> results = new ArrayList<BulkItemResponse>(1);
		Connection connection = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			final StringBuilder createTableQuery = IndexUtils.POOLED_STRING_BUILDER.get();
			createTableQuery.append("CREATE UNLOGGED TABLE ");
			createTableQuery.append(stagingTable);
			createTableQuery.append(" (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, ");
			createTableQuery.append("_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb)");
			if (tablespace != null && !tablespace.isEmpty()) {
				createTableQuery.append(" TABLESPACE ");
				createTableQuery.append(tablespace);
			}
			createTableQuery.append(" WITH (autovacuum_enabled=false)");

			PreparedStatement createTableStatement = connection.prepareStatement(createTableQuery.toString());
			createTableStatement.execute();
			createTableStatement.close();

			connection.setAutoCommit(false);

			final PgConnection pgConnection = connection.unwrap(PgConnection.class);
			final CopyManager copyManager = new CopyManager(pgConnection);

			final CopyIn copyIn = copyManager.
					copyIn("COPY " + stagingTable + " FROM STDIN WITH CSV ENCODING 'UTF8' DELIMITER " + DELIMITER_INIT + " QUOTE " + ESCAPE_INIT);

			try {
				final Timer.Context psqlTime = psqlTimer.time();
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
						final Timer.Context flattenTime = flattenTimer.time();
						escapedJson = IndexUtils.flattenJson(indexOperation.getSource());
						flattenTime.stop();
					} else {
						final Timer.Context escapeTime = flattenTimer.time();
						escapedJson = IndexUtils.psqlEscapeString(indexOperation.getSource());
						escapeTime.stop();
					}

					final StringBuilder rowBuilder = IndexUtils.POOLED_STRING_BUILDER.get();
					rowBuilder.append(indexOperation.getIndex());
					rowBuilder.append(DELIMITER);
					rowBuilder.append(indexOperation.getType());
					rowBuilder.append(DELIMITER);
					rowBuilder.append(indexOperation.getId());
					rowBuilder.append(DELIMITER);
					rowBuilder.append(indexOperation.getTimestamp());
					rowBuilder.append(DELIMITER);
					rowBuilder.append(bucket1s);
					rowBuilder.append(DELIMITER);
					rowBuilder.append(bucket1m);
					rowBuilder.append(DELIMITER);
					rowBuilder.append(bucket1h);
					rowBuilder.append(DELIMITER);
					rowBuilder.append(bucket1d);
					rowBuilder.append(DELIMITER);
					rowBuilder.append(ESCAPE);
					rowBuilder.append(escapedJson);
					rowBuilder.append(ESCAPE);
					rowBuilder.append(NEW_LINE);

					final byte [] rowBytes = rowBuilder.toString().getBytes(CHARSET);
					copyIn.writeToCopy(rowBytes, 0, rowBytes.length);
				}
				copyIn.endCopy();
				connection.commit();
				psqlTime.stop();
			} catch (PSQLException e) {
				throw e;
			} catch (Exception e) {
				throw e;
			}

			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				BulkItemResponse responseEntry = createEntry(i, "index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId(), BulkItemResponse.STATUS_CREATED);
				results.add(responseEntry);
				indexOperation.release();
			}
			totalSuccess = results.size();
			totalFailed = 0;
		} catch (Exception e) {
			totalSuccess = 0;
			totalFailed = 0;

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
					totalFailed++;
					responseEntry = createEntry(i, "index", indexOperation.getIndex(), indexOperation.getType(),
							indexOperation.getId(), BulkItemResponse.STATUS_FAILED);
				} else {
					totalSuccess++;
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

	public int getTotalFailed() {
		return totalFailed;
	}

	public int getTotalSuccess() {
		return totalSuccess;
	}
}
