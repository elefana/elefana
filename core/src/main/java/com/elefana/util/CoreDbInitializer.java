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

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Component
public class CoreDbInitializer implements DbInitializer {
	private static final Logger LOGGER = LoggerFactory.getLogger(CoreDbInitializer.class);

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	public void initialiseDatabase() throws SQLException {
		createSqlFunctions();
		createDuplicateKeyTable();
		createMasterTableIfNotExists();
		createPartitionTrackingTableIfNotExists();
		createFieldStatsTablesIfNotExists();
		createIndexTemplatesTableIfNotExists();
	}

	public boolean isTableDistributed(String tableName) {
		if (!nodeSettingsService.isUsingCitus()) {
			return false;
		}
		List<Map<String, Object>> results = jdbcTemplate.queryForList(
				"SELECT column_to_column_name(logicalrelid, partkey) AS dist_col_name FROM pg_dist_partition WHERE logicalrelid='"
						+ tableName + "'::regclass;");
		return !results.isEmpty();
	}

	private void createSqlFunctions() {
		try {
			ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator(
					new ClassPathResource("/functions.sql", IndexFieldMappingService.class));
			resourceDatabasePopulator.setSeparator(ScriptUtils.EOF_STATEMENT_SEPARATOR);
			resourceDatabasePopulator.execute(jdbcTemplate.getDataSource());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void createDuplicateKeyTable() {
		try {
			Connection connection = jdbcTemplate.getDataSource().getConnection();

			final String createDuplicateKeyTableQuery = "CREATE TABLE IF NOT EXISTS elefana_duplicate_keys"
					+ " (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, "
					+ "_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb);";
			PreparedStatement preparedStatement = connection.prepareStatement(createDuplicateKeyTableQuery);
			preparedStatement.execute();
			preparedStatement.close();

			connection.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void createMasterTableIfNotExists() throws SQLException {
		if (nodeSettingsService.isUsingCitus()) {
			return;
		}

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement;

		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + IndexUtils.DATA_TABLE
				+ " (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, "
				+ "_bucket1s BIGINT, _bucket1m BIGINT, _bucket1h BIGINT, _bucket1d BIGINT, _source jsonb) PARTITION BY LIST (_index);";
		preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();

		connection.close();
	}

	private void createPartitionTrackingTableIfNotExists() throws SQLException {
		if (!nodeSettingsService.isMasterNode()) {
			return;
		}

		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + IndexUtils.PARTITION_TRACKING_TABLE
				+ " (_index VARCHAR(255) PRIMARY KEY, _partitionTable VARCHAR(255) NOT NULL);";
		PreparedStatement preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();

		connection.close();

		if (nodeSettingsService.isUsingCitus() && !isTableDistributed(IndexUtils.PARTITION_TRACKING_TABLE)) {
			jdbcTemplate.execute(
					"SELECT create_distributed_table('" + IndexUtils.PARTITION_TRACKING_TABLE + "', '_index');");
		}
	}

	private void createFieldStatsTablesIfNotExists() throws SQLException {
		if (!nodeSettingsService.isMasterNode()) {
			return;
		}

		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_mapping (_tracking_id VARCHAR(255) PRIMARY KEY, _index VARCHAR(255), _type VARCHAR(255), _mapping jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_capabilities (_index VARCHAR(255) PRIMARY KEY, _capabilities jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_stats (_index VARCHAR(255) PRIMARY KEY, _stats jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_names (_tracking_id VARCHAR(255) PRIMARY KEY, _index VARCHAR(255), _type VARCHAR(255), _field_names jsonb);");

		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_mapping")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_mapping', '_tracking_id');");
		}
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_field_capabilities")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_capabilities', '_index');");
		}
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_field_stats")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_stats', '_index');");
		}
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_field_names")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_names', '_tracking_id');");
		}
	}

	private void createIndexTemplatesTableIfNotExists() throws SQLException {
		if (!nodeSettingsService.isMasterNode()) {
			return;
		}

		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_template (_template_id VARCHAR(255) PRIMARY KEY, _index_pattern VARCHAR(255), _storage jsonb, _mappings jsonb);");

		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_template")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_template', '_template_id');");
		}
	}
}
