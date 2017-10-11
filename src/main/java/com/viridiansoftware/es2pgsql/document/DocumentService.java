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
package com.viridiansoftware.es2pgsql.document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pgsql.exception.DocumentAlreadyExistsException;
import com.viridiansoftware.es2pgsql.exception.NoSuchDocumentException;
import com.viridiansoftware.es2pgsql.util.TableUtils;

@Service
public class DocumentService {
	private static final Logger LOGGER = LoggerFactory.getLogger(DocumentService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();

	public Map<String, Object> get(String index, String type, String id) throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			String query = "SELECT * FROM " + TableUtils.sanitizeTableName(index) + " WHERE _type = ? AND _id = ?";
			SqlRowSet resultSet = jdbcTemplate.queryForRowSet(query, type, id);
			if(resultSet.next()) {
				result.put("_index", resultSet.getString("_index"));
				result.put("_type", resultSet.getString("_type"));
				result.put("_id", resultSet.getString("_id"));
				result.put("_version", 1);
				result.put("found", true);
				result.put("_source", objectMapper.readValue(resultSet.getString("_source"), Map.class));
			}
		} catch (Exception e) {
			e.printStackTrace();
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}

	public MultiGetResponse multiGetByRequestBody(String requestBody) throws Exception {
		Map<String, Object> request = objectMapper.readValue(requestBody, Map.class);
		List<Object> requestItems = (List<Object>) request.get("docs");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for (Object tmpRequestItem : requestItems) {
				Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("SELECT * FROM ");
				queryBuilder.append(TableUtils.sanitizeTableName((String) requestItem.get("_index")));
				if (requestItem.containsKey("_type") || requestItem.containsKey("_id")) {
					queryBuilder.append(" WHERE ");
				}

				if (requestItem.containsKey("_type")) {
					queryBuilder.append("_type = '");
					queryBuilder.append(requestItem.get("_type"));
					queryBuilder.append("'");

					if (requestItem.containsKey("_id")) {
						queryBuilder.append(" AND ");
					}
				}
				if (requestItem.containsKey("_id")) {
					queryBuilder.append("_id = '");
					queryBuilder.append(requestItem.get("_id"));
					queryBuilder.append("'");
				}

				try {
					SqlRowSet resultSet = jdbcTemplate.queryForRowSet(queryBuilder.toString());
					while (resultSet.next()) {
						GetResponse getResponse = new GetResponse();
						getResponse.set_index(resultSet.getString("_index"));
						getResponse.set_type(resultSet.getString("_type"));
						getResponse.set_id(resultSet.getString("_id"));
						getResponse.set_source(objectMapper.readValue(resultSet.getString("_source"), Map.class));
						result.getDocs().add(getResponse);
					}
				} catch (Exception e) {
					GetResponse getResponse = new GetResponse();
					getResponse.set_index((String) requestItem.get("_index"));
					getResponse.set_type((String) requestItem.get("_type"));
					getResponse.set_id((String) requestItem.get("_id"));
					getResponse.setFound(false);
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

	public MultiGetResponse multiGet() throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for (String tableName : tableUtils.listTables()) {
				SqlRowSet resultSet = jdbcTemplate
						.queryForRowSet("SELECT * FROM " + TableUtils.sanitizeTableName(tableName));
				while (resultSet.next()) {
					GetResponse getResponse = new GetResponse();
					getResponse.set_index(resultSet.getString("_index"));
					getResponse.set_type(resultSet.getString("_type"));
					getResponse.set_id(resultSet.getString("_id"));
					getResponse.set_source(objectMapper.readValue(resultSet.getString("_source"), Map.class));
					result.getDocs().add(getResponse);
				}
			}
		} catch (Exception e) {
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
			String[] indices = indexPattern.split(",");
			for (int i = 0; i < indices.length; i++) {
				for (String tableName : tableUtils.listTables(indices[i])) {
					SqlRowSet resultSet = jdbcTemplate
							.queryForRowSet("SELECT * FROM " + TableUtils.sanitizeTableName(tableName));
					while (resultSet.next()) {
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
			SqlRowSet resultSet = jdbcTemplate.queryForRowSet(
					"SELECT * FROM " + TableUtils.sanitizeTableName(index) + " WHERE _type = '?''", type);
			while (resultSet.next()) {
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

	public IndexApiResponse index(String index, String type, String id, String document, IndexOpType opType)
			throws Exception {
		tableUtils.ensureTableExists(index);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(document);

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("INSERT INTO ");
		queryBuilder.append(TableUtils.sanitizeTableName(index));
		queryBuilder.append(" AS i");
		queryBuilder.append(" (_index, _type, _id, _timestamp, _source) VALUES (?, ?, ?, ?, ?)");

		switch (opType) {
		case CREATE:
			queryBuilder.append(" ON CONFLICT DO NOTHING");
			break;
		case UPDATE:
			queryBuilder.append(
					" ON CONFLICT (_id) DO UPDATE SET _timestamp = EXCLUDED._timestamp, _source = EXCLUDED._source || i._source");
			break;
		case OVERWRITE:
		default:
			queryBuilder.append(
					" ON CONFLICT (_id) DO UPDATE SET _timestamp = EXCLUDED._timestamp, _source = EXCLUDED._source");
			break;

		}
		queryBuilder.append(";");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.toString());
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
			if (opType == IndexOpType.CREATE) {
				throw new DocumentAlreadyExistsException();
			}
			throw new RuntimeException("");
		}
	}
}
