/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.elefana.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;

import com.codahale.metrics.MetricRegistry;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.NodeSettingsService;

@Component
public class DbInitializer {
	private static final Logger LOGGER = LoggerFactory.getLogger(DbInitializer.class);

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private MetricRegistry metricRegistry;

	public void initialiseDatabase() throws SQLException {
		createMasterTableIfNotExists();
		createPartitionTrackingTableIfNotExists();
		createFieldStatsTablesIfNotExists();
		createIndexTemplatesTableIfNotExists();
	}

	public boolean isTableDistributed(String tableName) {
		List<Map<String, Object>> results = jdbcTemplate.queryForList(
				"SELECT column_to_column_name(logicalrelid, partkey) AS dist_col_name FROM pg_dist_partition WHERE logicalrelid='"
						+ tableName + "'::regclass;");
		return !results.isEmpty();
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
		
		if (nodeSettingsService.isUsingCitus()) {
			return;
		}

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement;
		
		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + IndexUtils.DATA_TABLE
				+ " (_index VARCHAR(255) NOT NULL, _type VARCHAR(255) NOT NULL, _id VARCHAR(255) NOT NULL, _timestamp BIGINT, _source jsonb) PARTITION BY LIST (_index);";
		preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();
		
		connection.close();
	}

	private void createPartitionTrackingTableIfNotExists() throws SQLException {
		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createMasterTableQuery = "CREATE TABLE IF NOT EXISTS " + IndexUtils.PARTITION_TRACKING_TABLE
				+ " (_index VARCHAR(255) PRIMARY KEY, _partitionTable VARCHAR(255) NOT NULL);";
		PreparedStatement preparedStatement = connection.prepareStatement(createMasterTableQuery);
		preparedStatement.execute();
		preparedStatement.close();

		if (nodeSettingsService.isUsingCitus() && !isTableDistributed(IndexUtils.PARTITION_TRACKING_TABLE)) {
			preparedStatement = connection.prepareStatement(
					"SELECT create_distributed_table('" + IndexUtils.PARTITION_TRACKING_TABLE + "', '_index');");
			preparedStatement.execute();
			preparedStatement.close();
		}

		connection.close();
	}

	private void createFieldStatsTablesIfNotExists() throws SQLException {
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_mapping (_tracking_id VARCHAR(255) PRIMARY KEY, _index VARCHAR(255), _type VARCHAR(255), _mapping jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_capabilities (_index VARCHAR(255) PRIMARY KEY, _capabilities jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_stats (_index VARCHAR(255) PRIMARY KEY, _stats jsonb);");
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_mapping")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_mapping', '_tracking_id');");
		}
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_field_capabilities")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_capabilities', '_index');");
		}
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_field_stats")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_stats', '_index');");
		}
	}
	
	private void createIndexTemplatesTableIfNotExists() throws SQLException {
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_template (_template_id VARCHAR(255) PRIMARY KEY, _index_pattern VARCHAR(255), _timestamp_path VARCHAR(255), _mappings jsonb);");
		
		if (nodeSettingsService.isUsingCitus() && !isTableDistributed("elefana_index_template")) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_template', '_template_id');");
		}
	}
}
