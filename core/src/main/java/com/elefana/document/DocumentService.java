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
package com.elefana.document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import com.elefana.exception.DocumentAlreadyExistsException;
import com.elefana.exception.NoSuchDocumentException;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsoniter.JsonIterator;

@Service
public class DocumentService {
	private static final Logger LOGGER = LoggerFactory.getLogger(DocumentService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils tableUtils;
	@Autowired
	private VersionInfoService versionInfoService;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;

	private final ObjectMapper objectMapper = new ObjectMapper();

	public Map<String, Object> get(String index, String type, String id) throws Exception {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			String query = "SELECT * FROM " + IndexUtils.DATA_TABLE + " WHERE _index = ? AND _type = ? AND _id = ?";
			SqlRowSet resultSet = jdbcTemplate.queryForRowSet(query, index, type, id);
			
			result.put("_index", index);
			result.put("_type", type);
			result.put("_id", id);
			
			if(resultSet.next()) {
				result.put("_version", 1);
				result.put("found", true);
				result.put("_source", objectMapper.readValue(resultSet.getString("_source"), Map.class));
			} else {
				result.put("found", false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}

	public MultiGetResponse multiGet(String requestBody) throws Exception {
		Map<String, Object> request = objectMapper.readValue(requestBody, Map.class);
		List<Object> requestItems = (List<Object>) request.get("docs");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for (Object tmpRequestItem : requestItems) {
				Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("SELECT * FROM ");
				queryBuilder.append(IndexUtils.DATA_TABLE);
				queryBuilder.append(" WHERE ");
				
				if (requestItem.containsKey("_index")) {
					queryBuilder.append("_index = '");
					queryBuilder.append(requestItem.get("_index"));
					queryBuilder.append("'");

					if (requestItem.containsKey("_type") || requestItem.containsKey("_id")) {
						queryBuilder.append(" AND ");
					}
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

	public MultiGetResponse multiGet(String indexPattern, String requestBody) throws Exception {
		Map<String, Object> request = objectMapper.readValue(requestBody, Map.class);
		List<Object> requestItems = (List<Object>) request.get("docs");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for (String index : tableUtils.listIndicesForIndexPattern(indexPattern)) {
				for (Object tmpRequestItem : requestItems) {
					Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
					StringBuilder queryBuilder = new StringBuilder();
					queryBuilder.append("SELECT * FROM ");
					queryBuilder.append(IndexUtils.DATA_TABLE);
					queryBuilder.append(" WHERE ");
					queryBuilder.append("_index='");
					queryBuilder.append(index);
					queryBuilder.append("'");
					
					if (requestItem.containsKey("_type") || requestItem.containsKey("_id")) {
						queryBuilder.append(" AND ");
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
			}
		} catch (Exception e) {
			e.printStackTrace();
			connection.close();
			throw new NoSuchDocumentException();
		}
		connection.close();
		return result;
	}

	public MultiGetResponse multiGet(String indexPattern, String typePattern, String requestBody) throws Exception {
		Map<String, Object> request = objectMapper.readValue(requestBody, Map.class);
		List<Object> requestItems = (List<Object>) request.get("docs");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		MultiGetResponse result = new MultiGetResponse();
		try {
			for (String index : tableUtils.listIndicesForIndexPattern(indexPattern)) {
				for(String type : indexFieldMappingService.getTypesForIndex(index, typePattern)) {
					for (Object tmpRequestItem : requestItems) {
						Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
						StringBuilder queryBuilder = new StringBuilder();
						queryBuilder.append("SELECT * FROM ");
						queryBuilder.append(IndexUtils.DATA_TABLE);
						queryBuilder.append(" WHERE ");
						queryBuilder.append("_index = '");
						queryBuilder.append(index);
						queryBuilder.append("'");
						
						if (!type.isEmpty() || requestItem.containsKey("_id")) {
							queryBuilder.append(" AND ");
						}

						if (!type.isEmpty()) {
							queryBuilder.append("_type = '");
							queryBuilder.append(type);
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

	public IndexApiResponse index(String index, String type, String id, String document, IndexOpType opType)
			throws Exception {
		tableUtils.ensureIndexExists(index);
		
		switch(versionInfoService.getApiVersion()) {
		case V_2_4_3:
			switch (opType) {
			case UPDATE:
				document = JsonIterator.deserialize(document).get("doc").toString();
				break;
			case CREATE:
			case OVERWRITE:
			default:
				break;
			}
			break;
		case V_5_5_2:
		default:
			break;
		}

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(document);

		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT ");
		switch (opType) {
		case CREATE:
			queryBuilder.append("elefana_create");
			break;
		case UPDATE:
			queryBuilder.append("elefana_update");
			break;
		case OVERWRITE:
		default:
			queryBuilder.append("elefana_overwrite");
			break;
		}
		queryBuilder.append("(?, ?, ?, ?, ?);");

		Connection connection = jdbcTemplate.getDataSource().getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.toString());
		preparedStatement.setString(1, index);
		preparedStatement.setString(2, type);
		preparedStatement.setString(3, id);
		preparedStatement.setLong(4, System.currentTimeMillis());
		preparedStatement.setObject(5, jsonObject);
		
		ResultSet resultSet = preparedStatement.executeQuery();
		if (!resultSet.next()) {
			throw new RuntimeException("");
		}
		int rows = resultSet.getInt(1);
		preparedStatement.close();
		connection.close();

		if (rows > 0) {
			IndexApiResponse result = new IndexApiResponse();
			result._index = index;
			result._type = type;
			result._id = id;
			result._version = 1;
			result.created = true;
			
			indexFieldMappingService.scheduleIndexForMappingAndStats(index);
			return result;
		} else {
			if (opType == IndexOpType.CREATE) {
				throw new DocumentAlreadyExistsException();
			}
			throw new RuntimeException("");
		}
	}
}