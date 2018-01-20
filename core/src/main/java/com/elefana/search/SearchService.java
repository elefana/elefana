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

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.elefana.exception.ElefanaException;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;

@Service
public class SearchService {
	private static final Logger LOGGER = LoggerFactory.getLogger(SearchService.class);

	private static final String[] EMPTY_TYPES_LIST = new String[0];

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private MetricRegistry metricRegistry;

	private SearchQueryBuilder searchQueryBuilder;
	private Histogram searchTime, searchHits;

	@PostConstruct
	public void postConstruct() {
		searchTime = metricRegistry.histogram(MetricRegistry.name("search", "time"));
		searchHits = metricRegistry.histogram(MetricRegistry.name("search", "hits"));

		if (nodeSettingsService.isUsingCitus()) {
			searchQueryBuilder = new CitusSearchQueryBuilder(jdbcTemplate, indexUtils);
		} else {
			searchQueryBuilder = new PartitionTableSearchQueryBuilder(jdbcTemplate, indexUtils);
		}
	}
	
	public Map<String, Object> multiSearch(String httpRequest)
			throws ElefanaException {
		return multiSearch(null, null, httpRequest);
	}
	
	public Map<String, Object> multiSearch(String fallbackIndex, String httpRequest)
			throws ElefanaException {
		return multiSearch(fallbackIndex, null, httpRequest);
	}

	public Map<String, Object> multiSearch(String fallbackIndex, String fallbackType, String httpRequest)
			throws ElefanaException {
		String[] lines = httpRequest.split("\n");

		List<Map<String, Object>> responses = new ArrayList<Map<String, Object>>(1);

		for (int i = 0; i < lines.length; i += 2) {
			Map<String, Object> indexTypeInfo = JsonIterator.deserialize(lines[i], new TypeLiteral<Map<String, Object>>(){});
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

			responses.add(search(indices, types, lines[i + 1]));
		}

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("responses", responses);
		return result;
	}

	public Map<String, Object> search(String httpRequest) throws ElefanaException {
		return search(null, httpRequest);
	}

	public Map<String, Object> search(String indexPattern, String httpRequest) throws ElefanaException {
		return search(indexPattern, "", httpRequest);
	}

	public Map<String, Object> search(String indexPattern, String typesPattern, String httpRequest)
			throws ElefanaException {
		List<String> indices = indexPattern == null || indexPattern.isEmpty() ? indexUtils.listIndices()
				: indexUtils.listIndicesForIndexPattern(indexPattern);
		String[] types = typesPattern == null ? EMPTY_TYPES_LIST : typesPattern.split(",");
		return search(indices, types, httpRequest);
	}

	private Map<String, Object> search(List<String> indices, String[] types, String httpRequest)
			throws ElefanaException {
		final long startTime = System.currentTimeMillis();
		final RequestBodySearch requestBodySearch = new RequestBodySearch(httpRequest);
		if (!requestBodySearch.hasAggregations()) {
			return searchWithoutAggregation(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregation(indices, types, requestBodySearch, startTime);
	}

	private Map<String, Object> searchWithAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchQuery searchQuery = searchQueryBuilder.buildQuery(indices, types, requestBodySearch);
		final List<String> temporaryTablesCreated = searchQuery.getTemporaryTables();

		final Map<String, Object> result = executeQuery(searchQuery.getQuery(), startTime, requestBodySearch.getSize());
		final Map<String, Object> aggregationsResult = new HashMap<String, Object>();

		requestBodySearch.getAggregations().executeSqlQuery(indices, types, jdbcTemplate, nodeSettingsService,
				indexFieldMappingService, aggregationsResult, temporaryTablesCreated, searchQuery.getResultTable(),
				requestBodySearch);
		result.put("aggregations", aggregationsResult);
		return result;
	}

	private Map<String, Object> searchWithoutAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchQuery searchQuery = searchQueryBuilder.buildQuery(indices, types, requestBodySearch);
		final List<String> temporaryTablesCreated = searchQuery.getTemporaryTables();

		return executeQuery(searchQuery.getQuery(), startTime, requestBodySearch.getSize());
	}

	private Map<String, Object> executeQuery(String query, long startTime, int size) throws ElefanaException {
		LOGGER.info(query);
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
			throws ElefanaException {
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
				hit.put("_source", JsonIterator.deserialize(rowSet.getString("_source"), new TypeLiteral<Map<String, Object>>(){}));
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

		final long took = System.currentTimeMillis() - startTime;
		result.put("took", took);
		result.put("timed_out", false);
		result.put("hits", hits);
		result.put("_shards", shards);

		searchHits.update((int) hits.get("total"));
		searchTime.update(took);
		return result;
	}

	private Map<String, Object> getEmptySearchResult(long startTime, int size) throws ElefanaException {
		return convertSqlQueryResultToSearchResult(null, startTime, size);
	}
}
