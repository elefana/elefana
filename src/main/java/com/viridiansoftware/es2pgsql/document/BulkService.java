/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.document;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pgsql.util.TableUtils;

@Service
public class BulkService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BulkService.class);
	
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;

	public BulkApiResponse bulkOperations(String requestBody) throws Exception {
		final long startTime = System.currentTimeMillis();
		final BulkApiResponse bulkApiResponse = new BulkApiResponse();

		final String[] lines = requestBody.split("\n");

		final Map<String, List<BulkIndexOperation>> indexOperations = new HashMap<String, List<BulkIndexOperation>>();

		for (int i = 0; i < lines.length; i += 2) {
			Map<String, Object> operation = objectMapper.readValue(lines[i], Map.class);
			if (operation.containsKey("index")) {
				Map<String, Object> indexOperationTarget = (Map) operation.get("index");

				BulkIndexOperation indexOperation = BulkIndexOperation.allocate();
				indexOperation.setIndex((String) indexOperationTarget.get("_index"));
				indexOperation.setType((String) indexOperationTarget.get("_type"));
				if (indexOperationTarget.containsKey("_id")) {
					indexOperation.setId((String) indexOperationTarget.get("_id"));
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
			Collection<BulkIndexOperation> indexOperations) throws SQLException {
		tableUtils.ensureTableExists(index);
		
		Connection connection = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			final PgConnection pgConnection = connection.unwrap(PgConnection.class);
			final CopyManager copyManager = new CopyManager(pgConnection);

			CopyIn copyIn = copyManager
					.copyIn("COPY " + TableUtils.sanitizeTableName(index) + " FROM STDIN WITH DELIMITER '|'");

			for (BulkIndexOperation indexOperation : indexOperations) {
				final String row = indexOperation.getIndex() + "|" + indexOperation.getType() + "|"
						+ indexOperation.getId() + "|" + System.currentTimeMillis() + "|"  + indexOperation.getSource() + "\n";
				final byte [] rowBytes = row.getBytes();
				copyIn.writeToCopy(rowBytes, 0, rowBytes.length);

				Map<String, Object> responseEntry = bulkApiResponse.appendEntry("index", indexOperation.getIndex(),
						indexOperation.getType(), indexOperation.getId());
				responseEntry.put("result", "created");
				responseEntry.put("created", true);
				responseEntry.put("status", 201);
				indexOperation.release();
			}
			copyIn.endCopy();
			LOGGER.info("Indexed " + indexOperations.size() + " into index '" + index + "'");
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
	}
}
