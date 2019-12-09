/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

import com.codahale.metrics.Timer;
import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkOpType;
import com.elefana.api.document.DocumentShardInfo;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.json.JsonUtils;
import com.elefana.document.ingest.IngestTable;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.elefana.util.EscapeUtils;
import com.elefana.util.IndexUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class BulkIndexTask implements Callable<List<BulkItemResponse>> {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkIndexTask.class);

	protected static final String DELIMITER_INIT = "e'\\x1f'";
	protected static final String DELIMITER = Character.toString((char) 31);
	protected static final String ESCAPE_INIT = "e'\\x1e'";
	protected static final String ESCAPE = Character.toString((char) 30);
	protected static final String NEW_LINE = "\n";
	protected static final Charset CHARSET = Charset.forName("UTF-8");

	public static final String KEY_INDEX = "_index";
	public static final String KEY_TYPE = "_type";
	public static final String KEY_ID = "_id";

	protected static final long ONE_SECOND_IN_MILLIS = 1000L;
	protected static final long ONE_MINUTE_IN_MILLIS = ONE_SECOND_IN_MILLIS * 60L;
	protected static final long ONE_HOUR_IN_MILLIS = ONE_MINUTE_IN_MILLIS * 60L;
	protected static final long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24L;

	protected static final DocumentShardInfo SHARDS = new DocumentShardInfo();

	protected final Timer psqlTimer, batchBuildTimer, flattenTimer, escapeTimer;
	protected final JdbcTemplate jdbcTemplate;
	protected final List<BulkIndexOperation> indexOperations;
	protected final IngestTable ingestTable;
	protected final String index;
	protected final boolean flatten;
	protected final int from;
	protected final int size;

	protected final IndexFieldStatsService fieldStatsService;

	protected volatile int totalFailed, totalSuccess;

	public BulkIndexTask(JdbcTemplate jdbcTemplate, List<BulkIndexOperation> indexOperations,
						 String index, IngestTable ingestTable, boolean flatten, int from, int size,
						 Timer psqlTimer, Timer batchBuildTimer, Timer flattenTimer, Timer escapeTimer, IndexFieldStatsService fieldStatsService) {
		super();
		this.psqlTimer = psqlTimer;
		this.batchBuildTimer = batchBuildTimer;
		this.flattenTimer = flattenTimer;
		this.escapeTimer = escapeTimer;

		this.jdbcTemplate = jdbcTemplate;
		this.indexOperations = indexOperations;
		this.index = index;
		this.ingestTable = ingestTable;
		this.flatten = flatten;
		this.from = from;
		this.size = size;
		this.fieldStatsService = fieldStatsService;
	}

	protected abstract int getStagingTableId() throws ElefanaException;

	@Override
	public List<BulkItemResponse> call() {
		final List<BulkItemResponse> results = new ArrayList<BulkItemResponse>(1);
		Connection connection = null;

		final int stagingTableId;
		try {
			stagingTableId = getStagingTableId();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);

			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				BulkItemResponse responseEntry = createEntry(i, "index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId(), BulkItemResponse.STATUS_FAILED);
				results.add(responseEntry);
				indexOperation.release();
			}
			totalSuccess = 0;
			totalFailed = results.size();
			return results;
		}

		final String stagingTable = ingestTable.getIngestionTableName(stagingTableId);
		boolean stagingTableUnlocked = false;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();
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


					if(flatten) {
						final Timer.Context flattenTime = flattenTimer.time();
						IndexUtils.flattenJson(indexOperation);
						flattenTime.stop();
					} else {
						final Timer.Context escapeTime = flattenTimer.time();
						EscapeUtils.psqlEscapeString(indexOperation);
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
					rowBuilder.append(indexOperation.getDocument(), 0, indexOperation.getDocumentLength());
					rowBuilder.append(ESCAPE);
					rowBuilder.append(NEW_LINE);

					final byte [] rowBytes = rowBuilder.toString().getBytes(CHARSET);
					copyIn.writeToCopy(rowBytes, 0, rowBytes.length);
				}
				copyIn.endCopy();
				connection.commit();
				psqlTime.stop();

				// index is the only supported bulk operation, therefore always submit the document
				fieldStatsService.submitDocuments(indexOperations, from, size);

				ingestTable.markData(stagingTableId);
				ingestTable.unlockTable(stagingTableId);
				stagingTableUnlocked = true;
			} catch (PSQLException e) {
				connection.rollback();
				throw e;
			} catch (Exception e) {
				throw e;
			}

			for (int i = from; i < from + size && i < indexOperations.size(); i++) {
				BulkIndexOperation indexOperation = indexOperations.get(i);
				BulkItemResponse responseEntry = createEntry(i, "index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId(), BulkItemResponse.STATUS_CREATED);
				results.add(responseEntry);
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
						JsonUtils.fromJsonString(indexOperation.getDocument(), Map.class);
					} catch (Exception ex) {
						LOGGER.error("Invalid JSON: " + indexOperation.getDocument());
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
		if(!stagingTableUnlocked) {
			ingestTable.unlockTable(stagingTableId);
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

	public int getTotalFailed() {
		return totalFailed;
	}

	public int getTotalSuccess() {
		return totalSuccess;
	}
}
