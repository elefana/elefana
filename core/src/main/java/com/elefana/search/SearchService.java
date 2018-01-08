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
package com.elefana.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.util.IndexUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class SearchService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchService.class);

	private static final String SEARCH_TABLE_PREFIX = "es2pgsql_search_";
	private static final String[] EMPTY_TYPES_LIST = new String[0];

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;
	@Autowired
	private IndexUtils indexUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();

	public Map<String, Object> multiSearch(String fallbackIndex, String fallbackType, HttpEntity<String> httpRequest)
			throws Exception {
		String[] lines = httpRequest.getBody().split("\n");

		List<Map<String, Object>> responses = new ArrayList<Map<String, Object>>(1);

		for (int i = 0; i < lines.length; i += 2) {
			Map<String, Object> indexTypeInfo = objectMapper.readValue(lines[i], Map.class);
			indexTypeInfo.putIfAbsent("index", fallbackIndex);
			indexTypeInfo.putIfAbsent("type", fallbackType);

			List<String> indices = null;
			if (indexTypeInfo.get("index") instanceof List) {
				indices = indexUtils.listIndicesForIndexPattern((List) indexTypeInfo.get("index"));
			} else {
				indices = indexUtils.listIndicesForIndexPattern((String) indexTypeInfo.get("index"));
			}
			String[] types = null;
			if (indexTypeInfo.get("type") == null) {
				types = EMPTY_TYPES_LIST;
			} else if (indexTypeInfo.get("type") instanceof List) {
				types = (String[]) ((List) indexTypeInfo.get("type")).toArray(new String[0]);
			} else {
				types = ((String) indexTypeInfo.get("type")).split(",");
			}

			responses.add(search(indices, types, new HttpEntity<>(lines[i + 1])));
		}

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("responses", responses);
		return result;
	}

	public Map<String, Object> search(HttpEntity<String> httpRequest) throws Exception {
		return search(null, httpRequest);
	}

	public Map<String, Object> search(String indexPattern, HttpEntity<String> httpRequest) throws Exception {
		return search(indexPattern, "", httpRequest);
	}

	public Map<String, Object> search(String indexPattern, String typesPattern, HttpEntity<String> httpRequest)
			throws Exception {
		List<String> indices = indexPattern == null || indexPattern.isEmpty() ? indexUtils.listIndices()
				: indexUtils.listIndicesForIndexPattern(indexPattern);
		String[] types = typesPattern == null ? EMPTY_TYPES_LIST : typesPattern.split(",");
		return search(indices, types, httpRequest);
	}

	private Map<String, Object> search(List<String> indices, String[] types, HttpEntity<String> httpRequest)
			throws Exception {
		final long startTime = System.currentTimeMillis();
		final RequestBodySearch requestBodySearch = new RequestBodySearch(httpRequest.getBody());
		if (!requestBodySearch.hasAggregations()) {
			return searchWithoutAggregation(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregation(indices, types, requestBodySearch, startTime);
	}

	private Map<String, Object> searchWithAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		if(indices.isEmpty()) {
			return getEmptySearchResult(startTime, 0);
		}
		if (requestBodySearch.getQuery().isMatchAllQuery()) {
			if (requestBodySearch.getFrom() > 0) {
				return searchWithAggregationUsingFilteredTables(indices, types, requestBodySearch, startTime);
			} else if (requestBodySearch.getSize() > 0) {
				return searchWithAggregationUsingFilteredTables(indices, types, requestBodySearch, startTime);
			}
			return searchWithAggregationUsingOriginalTable(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregationUsingFilteredTables(indices, types, requestBodySearch, startTime);
	}

	private Map<String, Object> searchWithAggregationUsingOriginalTable(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		final List<String> temporaryTablesCreated = new ArrayList<String>(1);

		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT * FROM " + IndexUtils.DATA_TABLE);
		queryBuilder.append(" WHERE _index IN (");
		for(int i = 0; i < indices.size(); i++) {
			if(i > 0) {
				queryBuilder.append(',');
			}
			queryBuilder.append("'");
			queryBuilder.append(indices.get(i));
			queryBuilder.append("'");
		}
		queryBuilder.append(") ");
		
		if (!IndexUtils.isTypesEmpty(types)) {
			queryBuilder.append(" AND (");
			for (int j = 0; j < types.length; j++) {
				if (types[j].length() == 0) {
					continue;
				}
				if (j > 0) {
					queryBuilder.append(" OR ");
				}
				queryBuilder.append("_type = '");
				queryBuilder.append(types[j]);
				queryBuilder.append("'");
			}
			queryBuilder.append(")");
		}
		if (requestBodySearch.getSize() > 0) {
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(requestBodySearch.getSize());
		}
		if (requestBodySearch.getFrom() > 0) {
			queryBuilder.append(" OFFSET ");
			queryBuilder.append(requestBodySearch.getFrom());
		}

		final Map<String, Object> result = executeQuery(queryBuilder.toString(), startTime,
				requestBodySearch.getSize());
		final Map<String, Object> aggregationsResult = new HashMap<String, Object>();
		requestBodySearch.getAggregations().executeSqlQuery(indices, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
				temporaryTablesCreated, IndexUtils.DATA_TABLE, requestBodySearch);
		result.put("aggregations", aggregationsResult);
		return result;
	}

	private Map<String, Object> searchWithAggregationUsingFilteredTables(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		final List<String> temporaryTablesCreated = new ArrayList<String>(1);
		final String queryDataTableName = SEARCH_TABLE_PREFIX + requestBodySearch.hashCode();
		temporaryTablesCreated.add(queryDataTableName);

		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("CREATE TEMP TABLE ");
		queryBuilder.append(queryDataTableName);
		queryBuilder.append(" AS (");
		
		queryBuilder.append("SELECT * FROM ");
		queryBuilder.append(IndexUtils.DATA_TABLE);
		queryBuilder.append(" WHERE _index IN (");
		for(int i = 0; i < indices.size(); i++) {
			if(i > 0) {
				queryBuilder.append(',');
			}
			queryBuilder.append("'");
			queryBuilder.append(indices.get(i));
			queryBuilder.append("'");
		}
		queryBuilder.append(") ");
		
		if (!requestBodySearch.getQuery().isMatchAllQuery()) {
			queryBuilder.append(" AND (");
			queryBuilder.append(requestBodySearch.getQuerySqlWhereClause());
			queryBuilder.append(")");
		}
		
		if (!IndexUtils.isTypesEmpty(types)) {
			queryBuilder.append(" AND (");
			for (int j = 0; j < types.length; j++) {
				if (types[j].length() == 0) {
					continue;
				}
				if (j > 0) {
					queryBuilder.append(" OR ");
				}
				queryBuilder.append("_type = '");
				queryBuilder.append(types[j]);
				queryBuilder.append("'");
			}
			queryBuilder.append(")");
		}
		if (requestBodySearch.getSize() > 0) {
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(requestBodySearch.getSize());
		}
		if (requestBodySearch.getFrom() > 0) {
			queryBuilder.append(" OFFSET ");
			queryBuilder.append(requestBodySearch.getFrom());
		}
		queryBuilder.append(")");

		LOGGER.info(queryBuilder.toString());
		jdbcTemplate.update(queryBuilder.toString());

		final Map<String, Object> result = executeQuery("SELECT * FROM " + queryDataTableName, startTime,
				requestBodySearch.getSize());
		final Map<String, Object> aggregationsResult = new HashMap<String, Object>();

		requestBodySearch.getAggregations().executeSqlQuery(indices, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
				temporaryTablesCreated, queryDataTableName, requestBodySearch);
		result.put("aggregations", aggregationsResult);
		return result;
	}

	private Map<String, Object> searchWithoutAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		final StringBuilder queryBuilder = new StringBuilder();
		if (requestBodySearch.getSize() == 0) {
			queryBuilder.append("SELECT COUNT(*) FROM ");
		} else {
			queryBuilder.append("SELECT * FROM ");
		}
		queryBuilder.append(IndexUtils.DATA_TABLE);
		queryBuilder.append(" WHERE _index IN (");
		for(int i = 0; i < indices.size(); i++) {
			if(i > 0) {
				queryBuilder.append(',');
			}
			queryBuilder.append("'");
			queryBuilder.append(indices.get(i));
			queryBuilder.append("'");
		}
		queryBuilder.append(") ");
		
		if (!requestBodySearch.getQuery().isMatchAllQuery()) {
			queryBuilder.append(" AND (");
			queryBuilder.append(requestBodySearch.getQuerySqlWhereClause());
			queryBuilder.append(")");
		}
		if (!IndexUtils.isTypesEmpty(types)) {
			queryBuilder.append(" AND (");
			for (int j = 0; j < types.length; j++) {
				if (types[j].isEmpty()) {
					continue;
				}
				if (j > 0) {
					queryBuilder.append(" OR ");
				}
				queryBuilder.append("_type = '");
				queryBuilder.append(types[j]);
				queryBuilder.append("'");
			}
			queryBuilder.append(")");
		}
		if (requestBodySearch.getSize() > 0) {
			queryBuilder.append(" LIMIT ");
			queryBuilder.append(requestBodySearch.getSize());
		}
		if (requestBodySearch.getFrom() > 0) {
			queryBuilder.append(" OFFSET ");
			queryBuilder.append(requestBodySearch.getFrom());
		}
		LOGGER.info(queryBuilder.toString());
		return executeQuery(queryBuilder.toString(), startTime, requestBodySearch.getSize());
	}

	private Map<String, Object> executeQuery(String query, long startTime, int size) throws Exception {
		SqlRowSet sqlRowSet = null;
		try {
			sqlRowSet = jdbcTemplate.queryForRowSet(query);
			return convertSqlQueryResultToSearchResult(sqlRowSet, startTime, size);
		} catch (Exception e) {
			e.printStackTrace();
			if (e.getMessage().contains("No results")) {
				return convertSqlQueryResultToSearchResult(null, startTime, size);
			}
			throw e;
		}
	}

	private Map<String, Object> convertSqlQueryResultToSearchResult(SqlRowSet rowSet, long startTime, int size)
			throws Exception {
		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> hitsList = new ArrayList<Map<String, Object>>(1);

		final Map<String, Object> shards = new HashMap<String, Object>();
		shards.put("total", 1);
		shards.put("successful", 1);
		shards.put("failed", 0);
		
		final Map<String, Object> hits = new HashMap<String, Object>();

		if (rowSet == null) {
			hits.put("total", 0);
		} else if (size > 0) {
			int count = 0;
			while (rowSet.next() && count < size) {
				Map<String, Object> hit = new HashMap<String, Object>();
				hit.put("_index", rowSet.getString("_index"));
				hit.put("_type", rowSet.getString("_type"));
				hit.put("_id", rowSet.getString("_id"));
				hit.put("_score", 1.0);
				hit.put("_source",
						objectMapper.readValue(indexUtils.destringifyJson(rowSet.getString("_source")), Map.class));
				hitsList.add(hit);
				count++;
			}
			hits.put("total", hitsList.size());
		} else {
			int totalHits = 0;
			boolean hasCountColumn = true;
			while (rowSet.next()) {
				if (hasCountColumn) {
					try {
						totalHits += rowSet.getInt("count");
					} catch (Exception e) {
						hasCountColumn = false;
					}
				}
				if (!hasCountColumn) {
					totalHits++;
				}
			}
			hits.put("total", totalHits);
		}
		hits.put("max_score", 1.0);
		hits.put("hits", hitsList);

		result.put("took", System.currentTimeMillis() - startTime);
		result.put("timed_out", false);
		result.put("hits", hits);
		result.put("_shards", shards);
		return result;
	}
	
	private Map<String, Object> getEmptySearchResult(long startTime, int size) throws Exception {
		return convertSqlQueryResultToSearchResult(null, startTime, size);
	}
}
