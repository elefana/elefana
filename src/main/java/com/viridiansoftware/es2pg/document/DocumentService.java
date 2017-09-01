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
package com.viridiansoftware.es2pg.document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pg.util.TableUtils;

@Service
public class DocumentService {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();

	public IndexApiResponse index(String index, String type, String id, String document) throws Exception {
		tableUtils.ensureTableExists(index);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(document);

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement = connection
				.prepareStatement("INSERT INTO " + index + " (id, type, timestamp, data) VALUES (?, ?, ?, ?);");
		preparedStatement.setString(1, id);
		preparedStatement.setString(2, type);
		preparedStatement.setLong(3, System.currentTimeMillis());
		preparedStatement.setObject(4, jsonObject);
		int rows = preparedStatement.executeUpdate();

		if (rows > 0) {
			IndexApiResponse result = new IndexApiResponse();
			result._index = index;
			result._type = type;
			result._id = id;
			result._version = 1;
			result.created = true;
			return result;
		} else {
			throw new RuntimeException("");
		}
	}
}
