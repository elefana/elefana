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
package com.elefana.document.psql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.elefana.api.AckResponse;
import com.elefana.api.document.*;
import com.elefana.api.indices.DeleteIndexRequest;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.DocumentAlreadyExistsException;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.document.DocumentService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;

@Service
public class PsqlDocumentService implements DocumentService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlDocumentService.class);

	private static final long ONE_SECOND_IN_MILLIS = 1000L;
	private static final long ONE_MINUTE_IN_MILLIS = ONE_SECOND_IN_MILLIS * 60L;
	private static final long ONE_HOUR_IN_MILLIS = ONE_MINUTE_IN_MILLIS * 60L;
	private static final long ONE_DAY_IN_MILLIS = ONE_HOUR_IN_MILLIS * 24L;

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;
	@Autowired
	private PsqlIndexFieldMappingService indexFieldMappingService;

	private ExecutorService executorService;

	@PostConstruct
	public void postConstruct() {
		final int totalThreads = environment.getProperty("elefana.service.document.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		executorService = Executors.newFixedThreadPool(totalThreads);
	}

	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
	}

	@Override
	public GetRequest prepareGet(String index, String type, String id, boolean fetchSource) {
		final PsqlGetRequest getRequest = new PsqlGetRequest(this, index, type, id);
		getRequest.setFetchSource(fetchSource);
		return getRequest;
	}

	@Override
	public DeleteRequest prepareDelete(String index, String type, String id) {
		return new PsqlDeleteRequest(this, index, type, id);
	}

	@Override
	public DeleteIndexRequest prepareDeleteIndex(String indexPattern, String typePattern) {
		return new PsqlDeleteIndexRequest(this, indexPattern, typePattern);
	}

	@Override
	public MultiGetRequest prepareMultiGet(String requestBody) {
		return new PsqlMultiGetRequest(this, requestBody);
	}

	@Override
	public MultiGetRequest prepareMultiGet(String indexPattern, String requestBody) {
		final MultiGetRequest result = new PsqlMultiGetRequest(this, requestBody);
		result.setIndexPattern(indexPattern);
		return result;
	}

	@Override
	public MultiGetRequest prepareMultiGet(String indexPattern, String typePattern, String requestBody) {
		final MultiGetRequest result = new PsqlMultiGetRequest(this, requestBody);
		result.setIndexPattern(indexPattern);
		result.setTypePattern(typePattern);
		return result;
	}

	@Override
	public IndexRequest prepareIndex(String index, String type, String id, String document, IndexOpType opType) {
		final IndexRequest result = new PsqlIndexRequest(this);
		result.setIndex(index);
		result.setType(type);
		result.setId(id);
		result.setSource(document);
		result.setOpType(opType);
		return result;
	}

	public GetResponse get(GetRequest getRequest) throws ElefanaException {
		final GetResponse result = new GetResponse();
		result.setIndex(getRequest.getIndex());
		result.setType(getRequest.getType());
		result.setId(getRequest.getId());

		try {
			final String queryTarget = indexUtils.getQueryTarget(getRequest.getIndex());

			SqlRowSet resultSet;
			if (nodeSettingsService.isUsingCitus()) {
				if(getRequest.isFetchSource()) {
					String query = "SELECT * FROM " + queryTarget + " WHERE _type = ? AND _id = ?";
					resultSet = jdbcTemplate.queryForRowSet(query, getRequest.getType(), getRequest.getId());
				} else {
					String query = "SELECT _timestamp FROM " + queryTarget + " WHERE _type = ? AND _id = ?";
					resultSet = jdbcTemplate.queryForRowSet(query, getRequest.getType(), getRequest.getId());
				}
			} else {
				if(getRequest.isFetchSource()) {
					String query = "SELECT * FROM " + queryTarget + " WHERE _index = ? AND _type = ? AND _id = ?";
					resultSet = jdbcTemplate.queryForRowSet(query, getRequest.getIndex(), getRequest.getType(), getRequest.getId());
				} else {
					String query = "SELECT _timestamp FROM " + queryTarget + " WHERE _index = ? AND _type = ? AND _id = ?";
					resultSet = jdbcTemplate.queryForRowSet(query, getRequest.getIndex(), getRequest.getType(), getRequest.getId());
				}
			}

			if (resultSet.next()) {
				result.setVersion(1);
				result.setFound(true);

				if(getRequest.isFetchSource()) {
					result.setSource(JsonIterator.deserialize(IndexUtils.psqlUnescapeString(resultSet.getString("_source")),
							new TypeLiteral<Map<String, Object>>() {
							}));
				} else {
					result.setSource(new HashMap<String, Object>());
				}
			} else {
				result.setFound(false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			result.setFound(false);
		}
		return result;
	}

	public MultiGetResponse multiGet(String requestBody) throws ElefanaException {
		Map<String, Object> request = JsonIterator.deserialize(requestBody, new TypeLiteral<Map<String, Object>>() {
		});
		List<Object> requestItems = (List<Object>) request.get("docs");

		MultiGetResponse result = new MultiGetResponse();
		for (Object tmpRequestItem : requestItems) {
			Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;

			final String index = (String) requestItem.get("_index");
			final String type = requestItem.containsKey("_type") ? (String) requestItem.get("_type") : null;
			final String id = requestItem.containsKey("_id") ? (String) requestItem.get("_id") : null;

			try {
				final String queryTarget = indexUtils.getQueryTarget(index);

				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("SELECT * FROM ");
				queryBuilder.append(queryTarget);
				queryBuilder.append(" WHERE ");

				if (!nodeSettingsService.isUsingCitus()) {
					queryBuilder.append("_index = '");
					queryBuilder.append(index);
					queryBuilder.append("'");

					if (requestItem.containsKey("_type") || requestItem.containsKey("_id")) {
						queryBuilder.append(" AND ");
					}
				}

				if (type != null) {
					queryBuilder.append("_type = '");
					queryBuilder.append(type);
					queryBuilder.append("'");

					if (requestItem.containsKey("_id")) {
						queryBuilder.append(" AND ");
					}
				}

				if (id != null) {
					queryBuilder.append("_id = '");
					queryBuilder.append(id);
					queryBuilder.append("'");
				}

				try {
					SqlRowSet resultSet = jdbcTemplate.queryForRowSet(queryBuilder.toString());
					while (resultSet.next()) {
						GetResponse getResponse = new GetResponse();
						getResponse.setIndex(resultSet.getString("_index"));
						getResponse.setType(resultSet.getString("_type"));
						getResponse.setId(resultSet.getString("_id"));
						getResponse.setSource(
								JsonIterator.deserialize(IndexUtils.psqlUnescapeString(resultSet.getString("_source")),
										new TypeLiteral<Map<String, Object>>() {
										}));
						result.getDocs().add(getResponse);
					}
				} catch (Exception e) {
					GetResponse getResponse = new GetResponse();
					getResponse.setIndex((String) requestItem.get("_index"));
					getResponse.setType((String) requestItem.get("_type"));
					getResponse.setId((String) requestItem.get("_id"));
					getResponse.setFound(false);
					result.getDocs().add(getResponse);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new ShardFailedException(e);
			}
		}
		return result;
	}

	public MultiGetResponse multiGet(String indexPattern, String requestBody) throws ElefanaException {
		Map<String, Object> request = JsonIterator.deserialize(requestBody, new TypeLiteral<Map<String, Object>>() {
		});
		List<Object> requestItems = (List<Object>) request.get("docs");

		MultiGetResponse result = new MultiGetResponse();
		try {
			for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
				final String queryTarget = indexUtils.getQueryTarget(index);

				for (Object tmpRequestItem : requestItems) {
					Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
					StringBuilder queryBuilder = new StringBuilder();
					queryBuilder.append("SELECT * FROM ");
					queryBuilder.append(queryTarget);
					queryBuilder.append(" WHERE ");

					if (!nodeSettingsService.isUsingCitus()) {
						queryBuilder.append("_index='");
						queryBuilder.append(index);
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
							getResponse.setIndex(resultSet.getString("_index"));
							getResponse.setType(resultSet.getString("_type"));
							getResponse.setId(resultSet.getString("_id"));
							getResponse.setSource(JsonIterator.deserialize(
									IndexUtils.psqlUnescapeString(resultSet.getString("_source")),
									new TypeLiteral<Map<String, Object>>() {
									}));
							result.getDocs().add(getResponse);
						}
					} catch (Exception e) {
						GetResponse getResponse = new GetResponse();
						getResponse.setIndex((String) requestItem.get("_index"));
						getResponse.setType((String) requestItem.get("_type"));
						getResponse.setId((String) requestItem.get("_id"));
						getResponse.setFound(false);
						result.getDocs().add(getResponse);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
		return result;
	}

	public MultiGetResponse multiGet(String indexPattern, String typePattern, String requestBody)
			throws ElefanaException {
		Map<String, Object> request = JsonIterator.deserialize(requestBody, new TypeLiteral<Map<String, Object>>() {
		});
		List<Object> requestItems = (List<Object>) request.get("docs");

		MultiGetResponse result = new MultiGetResponse();
		try {
			for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
				final String queryTarget = indexUtils.getQueryTarget(index);

				for (String type : indexFieldMappingService.getTypesForIndex(index, typePattern)) {
					for (Object tmpRequestItem : requestItems) {
						Map<String, Object> requestItem = (Map<String, Object>) tmpRequestItem;
						StringBuilder queryBuilder = new StringBuilder();
						queryBuilder.append("SELECT * FROM ");
						queryBuilder.append(queryTarget);
						queryBuilder.append(" WHERE ");

						if (!nodeSettingsService.isUsingCitus()) {
							queryBuilder.append("_index = '");
							queryBuilder.append(index);
							queryBuilder.append("'");

							if (!type.isEmpty() || requestItem.containsKey("_id")) {
								queryBuilder.append(" AND ");
							}
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
								getResponse.setIndex(resultSet.getString("_index"));
								getResponse.setType(resultSet.getString("_type"));
								getResponse.setId(resultSet.getString("_id"));
								getResponse.setSource(JsonIterator.deserialize(
										IndexUtils.psqlUnescapeString(resultSet.getString("_source")),
										new TypeLiteral<Map<String, Object>>() {
										}));
								result.getDocs().add(getResponse);
							}
						} catch (Exception e) {
							LOGGER.error(e.getMessage(), e);
							GetResponse getResponse = new GetResponse();
							getResponse.setIndex((String) requestItem.get("_index"));
							getResponse.setType((String) requestItem.get("_type"));
							getResponse.setId((String) requestItem.get("_id"));
							getResponse.setFound(false);
							result.getDocs().add(getResponse);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
		return result;
	}

	public AckResponse deleteIndex(String indexPattern, String typePattern) throws ElefanaException {
		int rows = 0;

		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			final String queryTarget = indexUtils.getQueryTarget(index);

			if(typePattern.equals("*") && nodeSettingsService.isUsingCitus()) {
				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("TRUNCATE ");
				queryBuilder.append(queryTarget);

				try {
					LOGGER.info(queryBuilder.toString());
					rows += jdbcTemplate.update(queryBuilder.toString());
					if(rows > 0) {
						indexFieldMappingService.scheduleIndexForMappingAndStats(index);
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			} else {
				for (String type : indexFieldMappingService.getTypesForIndex(index, typePattern)) {
					StringBuilder queryBuilder = new StringBuilder();
					queryBuilder.append("DELETE FROM ");
					queryBuilder.append(queryTarget);
					queryBuilder.append(" WHERE ");

					if (!nodeSettingsService.isUsingCitus()) {
						queryBuilder.append("_index = '");
						queryBuilder.append(index);
						queryBuilder.append("'");
						queryBuilder.append(" AND ");
					}

					queryBuilder.append("_type = '");
					queryBuilder.append(type);
					queryBuilder.append("'");

					try {
						LOGGER.info(queryBuilder.toString());
						rows += jdbcTemplate.update(queryBuilder.toString());
						if(rows > 0) {
							indexFieldMappingService.scheduleIndexForMappingAndStats(index);
						}
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
					}
				}
			}
		}

		final AckResponse response = new AckResponse();
		if(rows > 0) {
			response.setAcknowledged(true);
		} else {
			response.setAcknowledged(false);
		}
		return response;
	}

	public DeleteResponse delete(String index, String type, String id) {
		final String queryTarget = indexUtils.getQueryTarget(index);

		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("DELETE FROM ");
		queryBuilder.append(queryTarget);
		queryBuilder.append(" WHERE ");

		if (!nodeSettingsService.isUsingCitus()) {
			queryBuilder.append("_index = '");
			queryBuilder.append(index);
			queryBuilder.append("'");

			if ((type != null &&  !type.isEmpty()) || (id != null && !id.isEmpty()) ) {
				queryBuilder.append(" AND ");
			}
		}

		if(type != null && !type.isEmpty()) {
			queryBuilder.append("_type = '");
			queryBuilder.append(type);
			queryBuilder.append("'");

			if (id != null && !id.isEmpty()) {
				queryBuilder.append(" AND ");
			}
		}

		if (id != null && !id.isEmpty()) {
			queryBuilder.append("_id = '");
			queryBuilder.append(id);
			queryBuilder.append("'");
		}

		final DeleteResponse response = new DeleteResponse();
		response.setIndex(index);
		response.setType(type);
		response.setId(id);
		try {
			LOGGER.info(queryBuilder.toString());
			int rows = jdbcTemplate.update(queryBuilder.toString());
			if(rows == 0) {
				response.setResult("not_found");
			} else {
				indexFieldMappingService.scheduleIndexForMappingAndStats(index);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return response;
	}

	public IndexResponse index(String index, String type, String id, String document, IndexOpType opType)
			throws ElefanaException {
		indexUtils.ensureIndexExists(index);

		if (id == null) {
			id = indexUtils.generateDocumentId(index, type, document);
		}

		switch (versionInfoService.getApiVersion()) {
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
		if(nodeSettingsService.isFlattenJson()) {
			try {
				document = IndexUtils.flattenJson(document);
			} catch (IOException e) {
				throw new ShardFailedException(e);
			}
		}
		document = IndexUtils.psqlEscapeString(document);
		document = IndexUtils.psqlEscapeString(document);

		final long timestamp = indexUtils.getTimestamp(index, document);
		final long bucket1s = timestamp - (timestamp % ONE_SECOND_IN_MILLIS);
		final long bucket1m = timestamp - (timestamp % ONE_MINUTE_IN_MILLIS);
		final long bucket1h = timestamp - (timestamp % ONE_HOUR_IN_MILLIS);
		final long bucket1d = timestamp - (timestamp % ONE_DAY_IN_MILLIS);

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		try {
			jsonObject.setValue(document);
		} catch (SQLException e) {
			throw new ShardFailedException(e);
		}

		final StringBuilder queryBuilder = new StringBuilder();

		if (nodeSettingsService.isUsingCitus()) {
			queryBuilder.append("INSERT INTO ");
			queryBuilder.append(indexUtils.getQueryTarget(index));
			queryBuilder.append(" AS i");
			queryBuilder.append(
					" (_index, _type, _id, _timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d, _source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

			switch (opType) {
			case CREATE:
				queryBuilder.append(" ON CONFLICT DO NOTHING");
				break;
			case UPDATE:
				queryBuilder.append(
						" ON CONFLICT (_id) DO UPDATE SET _timestamp = EXCLUDED._timestamp, "
						+ "_bucket1s = EXCLUDED._bucket1s, _bucket1m = EXCLUDED._bucket1m,"
						+ " _bucket1h = EXCLUDED._bucket1h, _bucket1d = EXCLUDED._bucket1d, "
						+ "_source = i._source || EXCLUDED._source");
				break;
			case OVERWRITE:
			default:
				queryBuilder.append(
						" ON CONFLICT (_id) DO UPDATE SET _timestamp = EXCLUDED._timestamp, "
						+ "_bucket1s = EXCLUDED._bucket1s, _bucket1m = EXCLUDED._bucket1m, "
						+ "_bucket1h = EXCLUDED._bucket1h, _bucket1d = EXCLUDED._bucket1d, "
						+ "_source = EXCLUDED._source");
				break;
			}
		} else {
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
			queryBuilder.append("(?, ?, ?, ?, ?, ?, ?, ?, ?);");
		}

		int rows = 0;
		try {
			Connection connection = jdbcTemplate.getDataSource().getConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.toString());
			preparedStatement.setString(1, index);
			preparedStatement.setString(2, type);
			preparedStatement.setString(3, id);
			preparedStatement.setLong(4, timestamp);
			preparedStatement.setLong(5, bucket1s);
			preparedStatement.setLong(6, bucket1m);
			preparedStatement.setLong(7, bucket1h);
			preparedStatement.setLong(8, bucket1d);
			preparedStatement.setObject(9, jsonObject);

			if (nodeSettingsService.isUsingCitus()) {
				rows = preparedStatement.executeUpdate();
			} else {
				ResultSet resultSet = preparedStatement.executeQuery();
				if (!resultSet.next()) {
					preparedStatement.close();
					connection.close();
					throw new RuntimeException("");
				}
				rows = resultSet.getInt(1);
				resultSet.close();
			}

			preparedStatement.close();
			connection.close();
		} catch (Exception e) {
			LOGGER.error(document);
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		}

		if (rows > 0) {
			IndexResponse result = new IndexResponse();
			result.setIndex(index);
			result.setType(type);
			result.setId(id);
			result.setVersion(1);
			if (opType == IndexOpType.UPDATE) {
				result.setCreated(false);
			} else {
				result.setCreated(true);
			}

			indexFieldMappingService.scheduleIndexForMappingAndStats(index);
			return result;
		} else {
			if (opType == IndexOpType.CREATE) {
				throw new DocumentAlreadyExistsException(index, type, id);
			}
			throw new ShardFailedException();
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
