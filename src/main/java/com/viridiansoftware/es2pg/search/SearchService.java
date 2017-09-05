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
package com.viridiansoftware.es2pg.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pg.util.TableGarbageCollector;
import com.viridiansoftware.es2pg.util.TableUtils;

@Service
public class SearchService {
	private static final String SEARCH_TABLE_PREFIX = "search_";
	private static final String[] EMPTY_TYPES_LIST = new String[0];

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;
	@Autowired
	private TableGarbageCollector garbageCollector;
	
	private final ObjectMapper objectMapper = new ObjectMapper();

	public Map<String, Object> search(String transmittedRequestBody) throws Exception {
		return search(null, transmittedRequestBody);
	}

	public Map<String, Object> search(String indexPattern, String transmittedRequestBody) throws Exception {
		return search(indexPattern, "", transmittedRequestBody);
	}

	public Map<String, Object> search(String indexPattern, String typesPattern, String transmittedRequestBody)
			throws Exception {
		List<String> indices = indexPattern == null ? tableUtils.listTables() : tableUtils.listTables(indexPattern);
		String[] types = typesPattern == null ? EMPTY_TYPES_LIST : typesPattern.split(",");
		return search(indices, types, transmittedRequestBody);
	}

	private Map<String, Object> search(List<String> indices, String[] types, String transmittedRequestBody)
			throws Exception {
		final long startTime = System.currentTimeMillis();
		final RequestBodySearch requestBodySearch = new RequestBodySearch(transmittedRequestBody);
		if (!requestBodySearch.hasAggregations()) {
			return searchWithoutAggregation(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregation(indices, types, requestBodySearch, startTime);
	}
	
	private Map<String, Object> searchWithAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		final List<String> temporaryTablesCreated = new ArrayList<String>(1);
		final String queryDataTableName = SEARCH_TABLE_PREFIX + requestBodySearch.hashCode() + "_"
				+ System.currentTimeMillis();
		temporaryTablesCreated.add(queryDataTableName);

		StringBuilder tempTableBuilderQuery = new StringBuilder();
		for (int i = 0; i < indices.size(); i++) {
			if (i > 0) {
				tempTableBuilderQuery.append(" UNION ALL ");
			}
			tempTableBuilderQuery.append("SELECT * FROM ");
			tempTableBuilderQuery.append(indices.get(i));
			tempTableBuilderQuery.append(" WHERE ");
			tempTableBuilderQuery.append(requestBodySearch.getQuerySqlWhereClause());

			if (types.length <= 0) {
				tempTableBuilderQuery.append(" AND (");
				for (int j = 0; j < types.length; j++) {
					if (types[j].length() == 0) {
						continue;
					}
					if (j > 0) {
						tempTableBuilderQuery.append(" OR ");
					}
					tempTableBuilderQuery.append("type = '");
					tempTableBuilderQuery.append(types[j]);
					tempTableBuilderQuery.append("'");
				}
				tempTableBuilderQuery.append(")");
			}

		}
		tempTableBuilderQuery.append(" INTO ");
		tempTableBuilderQuery.append(queryDataTableName);

		jdbcTemplate.execute(tempTableBuilderQuery.toString());
		
		final String aggregationQuery = requestBodySearch.getAggregation().toSqlQuery(temporaryTablesCreated, queryDataTableName);
		final Map<String, Object> result = executeQuery(aggregationQuery, startTime);
		garbageCollector.scheduleTablesForDeletion(temporaryTablesCreated);
		return result;
	}

	private Map<String, Object> searchWithoutAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws Exception {
		final StringBuilder tempTableBuilderQuery = new StringBuilder();
		for (int i = 0; i < indices.size(); i++) {
			if (i > 0) {
				tempTableBuilderQuery.append(" UNION ALL ");
			}
			tempTableBuilderQuery.append("SELECT * FROM ");
			tempTableBuilderQuery.append(indices.get(i));
			tempTableBuilderQuery.append(" WHERE ");
			tempTableBuilderQuery.append(requestBodySearch.getQuerySqlWhereClause());
		}
		return executeQuery(tempTableBuilderQuery.toString(), startTime);
	}
	
	private Map<String, Object> executeQuery(String query, long startTime) throws Exception {
		final Map<String, Object> result = new HashMap<String, Object>();
		final List<Map<String, Object>> hitsList = new ArrayList<Map<String, Object>>(1);
		
		SqlRowSet rowSet = jdbcTemplate.queryForRowSet(query);
		while (rowSet.next()) {
			Map<String, Object> hit = new HashMap<String, Object>();
			hit.put("_index", rowSet.getString("_index"));
			hit.put("_type", rowSet.getString("_type"));
			hit.put("_id", rowSet.getString("_id"));
			hit.put("_score", 1.0);
			hit.put("_source", objectMapper.readValue(tableUtils.destringifyJson(rowSet.getString("_source")), Map.class));
			hitsList.add(hit);
		}

		Map<String, Object> hits = new HashMap<String, Object>();
		hits.put("total", hitsList.size());
		hits.put("max_score", 1.0);
		hits.put("hits", hitsList);

		result.put("took", System.currentTimeMillis() - startTime);
		result.put("timed_out", false);
		result.put("hits", hits);
		return result;
	}
}
