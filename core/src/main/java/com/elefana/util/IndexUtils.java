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
package com.elefana.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.NodeSettingsService;

/**
 *
 */
@Component
public class IndexUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexUtils.class);

	public static final String DATA_TABLE = "elefana_data";
	public static final String PARTITION_TRACKING_TABLE = "elefana_partition_tracking";
	
	public static final String TRIGGERS_PREFIX = "elefana_triggers_";
	public static final String GIN_INDEX_PREFIX = "elefana_gin_idx_";
	public static final String PRIMARY_KEY_PREFIX = "elefana_pkey_";
	
	private final Set<String> knownTables = new ConcurrentSkipListSet<String>();

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	@PostConstruct
	public void postConstruct() throws SQLException {
		createMasterTableIfNotExists();
		createPartitionTrackingTableIfNotExists();
		knownTables.addAll(listTables());
	}

	private void createMasterTableIfNotExists() throws SQLException {
		try {
			ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator(
					new ClassPathResource("/functions.sql", IndexFieldMappingService.class));
			resourceDatabasePopulator.setSeparator(ScriptUtils.EOF_STATEMENT_SEPARATOR);
			resourceDatabasePopulator.execute(jdbcTemplate.getDataSource());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		
		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + DATA_TABLE
				+ " (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, _source jsonb) PARTITION BY LIST (_index);";
		PreparedStatement preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();
		
		final String triggerName = TRIGGERS_PREFIX + DATA_TABLE;

		if (nodeSettingsService.isUsingCitus()) {
			preparedStatement = connection
					.prepareStatement("SELECT create_distributed_table('" + DATA_TABLE + "', '_id');");
			preparedStatement.execute();
			preparedStatement.close();
		}

		connection.close();
	}

	private void createPartitionTrackingTableIfNotExists() throws SQLException {
		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + PARTITION_TRACKING_TABLE
				+ " (_index VARCHAR(255) PRIMARY KEY, _partitionTable VARCHAR(255) NOT NULL);";
		PreparedStatement preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();

		if (nodeSettingsService.isUsingCitus()) {
			preparedStatement = connection
					.prepareStatement("SELECT create_distributed_table('" + PARTITION_TRACKING_TABLE + "', '_index');");
			preparedStatement.execute();
			preparedStatement.close();
		}

		connection.close();
	}

	public List<String> listIndices() throws SQLException {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE;
		final List<String> results = new ArrayList<String>(1);

		for (Map<String, Object> row : jdbcTemplate.queryForList(query)) {
			results.add((String) row.get("_index"));
		}
		return results;
	}
	
	public List<String> listIndicesForIndexPattern(List<String> indexPatterns) throws SQLException {
		Set<String> results = new HashSet<String>();
		for (String indexPattern : indexPatterns) {
			results.addAll(listIndicesForIndexPattern(indexPattern));
		}
		return new ArrayList<String>(results);
	}

	public List<String> listIndicesForIndexPattern(String indexPattern) throws SQLException {
		final List<String> results = listIndices();
		final String[] patterns = indexPattern.split(",");

		for (int i = 0; i < patterns.length; i++) {
			patterns[i] = patterns[i].toLowerCase();
			patterns[i] = patterns[i].replace("*", "(.*)");
			patterns[i] = "^" + patterns[i] + "$";
		}

		for (int i = results.size() - 1; i >= 0; i--) {
			boolean matchesPattern = false;

			for (int j = 0; j < patterns.length; j++) {
				if (results.get(i).toLowerCase().matches(patterns[j])) {
					matchesPattern = true;
					break;
				}
			}
			if (matchesPattern) {
				continue;
			}
			results.remove(i);
		}
		return results;
	}

	public void ensureIndexExists(String indexName) throws SQLException {
		final String tableName = convertIndexNameToTableName(indexName);
		if (knownTables.contains(tableName)) {
			return;
		}
		final String ginIndexName = GIN_INDEX_PREFIX + tableName;
		final String constraintName = PRIMARY_KEY_PREFIX + tableName;

		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " PARTITION OF " + DATA_TABLE
				+ " FOR VALUES in ('" + indexName + "')";
		LOGGER.info(createTableQuery);
		PreparedStatement preparedStatement = connection.prepareStatement(createTableQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createPrimaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName
				+ " PRIMARY KEY (_id);";
		LOGGER.info(createPrimaryKeyQuery);
		preparedStatement = connection.prepareStatement(createPrimaryKeyQuery);
		preparedStatement.execute();
		preparedStatement.close();

		final String createGinIndexQuery = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName
				+ " USING GIN (_source jsonb_ops)";
		LOGGER.info(createGinIndexQuery);
		preparedStatement = connection.prepareStatement(createGinIndexQuery);
		preparedStatement.execute();
		preparedStatement.close();
		
		final String createPartitionTrackingEntry = "INSERT INTO " + PARTITION_TRACKING_TABLE + " (_index, _partitionTable) VALUES (?, ?) ON CONFLICT DO NOTHING";
		preparedStatement = connection.prepareStatement(createPartitionTrackingEntry);
		preparedStatement.setString(1, indexName);
		preparedStatement.setString(2, tableName);
		preparedStatement.execute();
		preparedStatement.close();
		
		connection.close();
		knownTables.add(tableName);
	}

	public void deleteIndex(String indexName) {
		deleteTable(convertIndexNameToTableName(indexName));
	}

	private void deleteTable(String tableName) {
		jdbcTemplate.update("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
	}
	
	private List<String> listTables() throws SQLException {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE;
		final List<String> results = new ArrayList<String>(1);

		for (Map<String, Object> row : jdbcTemplate.queryForList(query)) {
			results.add((String) row.get("_partitionTable"));
		}
		return results;
	}
	
	public String getIndexForPartitionTable(String partitionTable) {
		final String query = "SELECT _index FROM " + PARTITION_TRACKING_TABLE + " WHERE _partitionTable = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, partitionTable)) {
			return (String) row.get("_index");
		}
		return null;
	}
	
	public String getPartitionTableForIndex(String index) {
		final String query = "SELECT _partitionTable FROM " + PARTITION_TRACKING_TABLE + " WHERE _index = ?";
		for (Map<String, Object> row : jdbcTemplate.queryForList(query, index)) {
			return (String) row.get("_partitionTable");
		}
		return null;
	}

	private static String convertIndexNameToTableName(String indexName) {
		indexName = indexName.replace(".", "_f_");
		indexName = indexName.replace("-", "_m_");
		indexName = indexName.replace(":", "_c_");

		if (!Character.isLetter(indexName.charAt(0))) {
			indexName = "_" + indexName;
		}
		return indexName;
	}

	private static String convertTableNameToIndexName(String tableName) {
		tableName = tableName.replace("_f_", ".");
		tableName = tableName.replace("_m_", "-");
		tableName = tableName.replace("_c_", ":");
		if (tableName.charAt(0) == '_') {
			return tableName.substring(1);
		}
		return tableName;
	}

	public static String destringifyJson(String json) {
		if (json.startsWith("\"")) {
			json = json.substring(1, json.length() - 1);
			json = json.replace("\\", "");
		}
		return json;
	}

	public static boolean isTypesEmpty(String[] types) {
		if (types == null) {
			return true;
		}
		if (types.length == 0) {
			return true;
		}
		for (int i = 0; i < types.length; i++) {
			if (types[i] == null) {
				continue;
			}
			if (types[i].isEmpty()) {
				continue;
			}
			return false;
		}
		return true;
	}
}