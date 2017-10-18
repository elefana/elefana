/**
 * Copyright 2017 Viridian Software Ltd.
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
 */
package com.viridiansoftware.es2pgsql.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.viridiansoftware.es2pgsql.document.BulkService;
import com.viridiansoftware.es2pgsql.node.NodeSettingsService;

@Component
public class TableUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);

	private final Set<String> knownTables = new ConcurrentSkipListSet<String>();

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static String sanitizeTableName(String tableName) {
		tableName = tableName.replace(".", "_f_");
		tableName = tableName.replace("-", "_m_");
		tableName = tableName.replace(":", "_c_");
		return tableName;
	}

	public List<String> listTables() throws SQLException {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();
		ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });

		List<String> results = new ArrayList<String>(1);
		while (resultSet.next()) {
			String tableName = resultSet.getString(3);
			if (tableName.startsWith("es2pgsql_")) {
				continue;
			}
			results.add(tableName);
		}
		connection.close();
		return results;
	}

	public List<String> listTables(List<String> tablePatterns) throws SQLException {
		Set<String> results = new HashSet<String>();
		for (String tablePattern : tablePatterns) {
			results.addAll(listTables(tablePattern));
		}
		return new ArrayList<String>(results);
	}

	public List<String> listTables(String tablePattern) throws SQLException {
		tablePattern = tablePattern.replace(".", "\\$");
		tablePattern = tablePattern.replace("*", "(.*)");
		tablePattern = "^" + tablePattern + "$";

		List<String> results = listTables();
		for (int i = results.size() - 1; i >= 0; i--) {
			if (results.get(i).matches(tablePattern)) {
				continue;
			}
			results.remove(i);
		}
		return results;
	}

	public void deleteTable(String tableName) {
		tableName = sanitizeTableName(tableName);
		jdbcTemplate.update("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
	}

	public void ensureTableExists(String tableName) throws SQLException {
		tableName = sanitizeTableName(tableName);
		if (knownTables.contains(tableName)) {
			return;
		}
		final String ginIndexName = "gin_index_" + tableName;

		Connection connection = jdbcTemplate.getDataSource().getConnection();

		final String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName
				+ " (_index VARCHAR(255), _type VARCHAR(255), _id VARCHAR(255) PRIMARY KEY, _timestamp BIGINT, _source jsonb)";
		LOGGER.info(createTableQuery);
		PreparedStatement preparedStatement = connection.prepareStatement(createTableQuery);
		preparedStatement.execute();

		final String createIndexQuery = "CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName
				+ " USING GIN (_source jsonb_ops)";
		LOGGER.info(createIndexQuery);
		preparedStatement = connection.prepareStatement(createIndexQuery);
		preparedStatement.execute();

		if (nodeSettingsService.isUsingCitus()) {
			preparedStatement = connection
					.prepareStatement("SELECT create_distributed_table('" + tableName + "', '_id');");
			preparedStatement.execute();
		} else {
			preparedStatement = connection
					.prepareStatement("DROP TRIGGER IF EXISTS es2pgsql_triggers_m_" + tableName + " ON " + tableName);
			preparedStatement.execute();

			preparedStatement = connection
					.prepareStatement("CREATE TRIGGER es2pgsql_triggers_m_" + tableName + " AFTER INSERT ON "
							+ tableName + " FOR EACH STATEMENT EXECUTE PROCEDURE es2pgsql_schedule_index_mapping();");
			preparedStatement.execute();
		}
		preparedStatement.close();
		connection.close();
		knownTables.add(tableName);
	}

	public static String destringifyJson(String json) {
		if (json.startsWith("\"")) {
			json = json.substring(1, json.length() - 1);
			json = json.replace("\\", "");
		}
		return json;
	}
}
