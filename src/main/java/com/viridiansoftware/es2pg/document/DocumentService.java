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
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pg.exception.NoSuchDocumentException;
import com.viridiansoftware.es2pg.util.TableUtils;

@Service
public class DocumentService {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();
	
	public Map<String, Object> get(String index, String type, String id) throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		Map<String, Object> result = null;
		try {
			result = jdbcTemplate.queryForMap("SELECT * FROM " + index + " WHERE _type = '?' AND _id = '?'", type, id);
		} catch (Exception e) {
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}
	
	public MultiGetResponse multiGet() throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for(String tableName : tableUtils.listTables()) {
				SqlRowSet resultSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + tableName + " LIMIT 10");
				while(resultSet.next()) {
					GetResponse getResponse = new GetResponse();
					getResponse.set_index(resultSet.getString("_index")); 
					getResponse.set_type(resultSet.getString("_type")); 
					getResponse.set_id(resultSet.getString("_id")); 
					getResponse.set_source(objectMapper.readValue(resultSet.getString("_source"), Map.class));
					result.getDocs().add(getResponse);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}
	
	public MultiGetResponse multiGet(String indexPattern) throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			String [] indices = indexPattern.split(",");
			for(int i = 0; i < indices.length; i++) {
				for(String tableName : tableUtils.listTables(indices[i])) {
					SqlRowSet resultSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + tableName);
					while(resultSet.next()) {
						GetResponse getResponse = new GetResponse();
						getResponse.set_index(resultSet.getString("_index")); 
						getResponse.set_type(resultSet.getString("_type")); 
						getResponse.set_id(resultSet.getString("_id")); 
						getResponse.set_source(objectMapper.readValue(resultSet.getString("_source"), Map.class));
						result.getDocs().add(getResponse);
					}
				}
			}
		} catch (Exception e) {
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}
	
	public MultiGetResponse multiGet(String index, String type) throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			SqlRowSet resultSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + index + " WHERE _type = '?''", type);
			while(resultSet.next()) {
				GetResponse getResponse = new GetResponse();
				getResponse.set_index(resultSet.getString("_index")); 
				getResponse.set_type(resultSet.getString("_type")); 
				getResponse.set_id(resultSet.getString("_id")); 
				getResponse.set_source(objectMapper.readValue(resultSet.getString("_source"), Map.class));
				result.getDocs().add(getResponse);
			}
		} catch (Exception e) {
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}

	public IndexApiResponse index(String index, String type, String id, String document) throws Exception {
		tableUtils.ensureTableExists(index);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(document);

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement = connection
				.prepareStatement("INSERT INTO " + index + " (_index, _type, _id, _timestamp, _source) VALUES (?, ?, ?, ?, ?);");
		preparedStatement.setString(1, index);
		preparedStatement.setString(2, type);
		preparedStatement.setString(3, id);
		preparedStatement.setLong(4, System.currentTimeMillis());
		preparedStatement.setObject(5, jsonObject);
		int rows = preparedStatement.executeUpdate();
		connection.close();
		
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
