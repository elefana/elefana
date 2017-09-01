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
package com.viridiansoftware.es2pg.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class TableUtils {
	private final Set<String> knownTables = new ConcurrentSkipListSet<String>();

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private boolean usingCitus = false;

	@PostConstruct
	public void postConstruct() {
		usingCitus = environment.getProperty("es2pg.citus", Boolean.class, false);
	}

	public void ensureTableExists(final String tableName) throws SQLException {
		if (knownTables.contains(tableName)) {
			return;
		}
		final String ginIndexName = "gin_index_" + tableName;

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(
				"CREATE TABLE IF NOT EXISTS " + tableName + " (id VARCHAR(255) PRIMARY KEY, type VARCHAR(255), timestamp BIGINT, data jsonb)");
		preparedStatement.execute();

		preparedStatement = connection
				.prepareStatement("CREATE INDEX IF NOT EXISTS " + ginIndexName + " ON " + tableName + " USING GIN (data jsonb_ops);");
		preparedStatement.execute();

		if (usingCitus) {
			preparedStatement = connection.prepareStatement("SELECT create_distributed_table('" + tableName + "', 'id');");
			preparedStatement.execute();
		}
		connection.close();
		knownTables.add(tableName);
	}
}
