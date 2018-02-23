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
package com.elefana.search.psql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.search.MultiSearchRequest;
import com.elefana.api.search.MultiSearchResponse;
import com.elefana.api.search.SearchHit;
import com.elefana.api.search.SearchRequest;
import com.elefana.api.search.SearchResponse;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.CitusSearchQueryBuilder;
import com.elefana.search.PartitionTableSearchQueryBuilder;
import com.elefana.search.RequestBodySearch;
import com.elefana.search.SearchQuery;
import com.elefana.search.SearchQueryBuilder;
import com.elefana.search.SearchService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;

@Service
public class PsqlSearchService implements SearchService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlSearchService.class);

	private static final String[] EMPTY_TYPES_LIST = new String[0];

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private PsqlIndexFieldMappingService indexFieldMappingService;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private MetricRegistry metricRegistry;

	private ExecutorService executorService;
	private SearchQueryBuilder searchQueryBuilder;
	private Histogram searchTime, searchHits;

	@PostConstruct
	public void postConstruct() {
		final int totalThreads = environment.getProperty("elefana.service.search.threads", Integer.class, Runtime.getRuntime().availableProcessors());
		executorService = Executors.newFixedThreadPool(totalThreads);
		
		searchTime = metricRegistry.histogram(MetricRegistry.name("search", "time"));
		searchHits = metricRegistry.histogram(MetricRegistry.name("search", "hits"));

		if (nodeSettingsService.isUsingCitus()) {
			searchQueryBuilder = new CitusSearchQueryBuilder(jdbcTemplate, indexUtils);
		} else {
			searchQueryBuilder = new PartitionTableSearchQueryBuilder(jdbcTemplate, indexUtils);
		}
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
	}
	
	public MultiSearchResponse multiSearch(String httpRequest)
			throws ElefanaException {
		return multiSearch(null, null, httpRequest);
	}
	
	public MultiSearchResponse multiSearch(String fallbackIndex, String httpRequest)
			throws ElefanaException {
		return multiSearch(fallbackIndex, null, httpRequest);
	}

	public MultiSearchResponse multiSearch(String fallbackIndex, String fallbackType, String httpRequest)
			throws ElefanaException {
		String[] lines = httpRequest.split("\n");

		final MultiSearchResponse result = new MultiSearchResponse();
		
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

			result.getResponses().add(internalSearch(indices, types, lines[i + 1]));
		}

		return result;
	}

	public SearchResponse search(String httpRequest) throws ElefanaException {
		return search(null, null, httpRequest);
	}

	public SearchResponse search(String indexPattern, String httpRequest) throws ElefanaException {
		return search(indexPattern, "", httpRequest);
	}

	public SearchResponse search(String indexPattern, String typesPattern, String httpRequest)
			throws ElefanaException {
		List<String> indices = indexPattern == null || indexPattern.isEmpty() ? indexUtils.listIndices()
				: indexUtils.listIndicesForIndexPattern(indexPattern);
		String[] types = typesPattern == null ? EMPTY_TYPES_LIST : typesPattern.split(",");
		return internalSearch(indices, types, httpRequest);
	}

	private SearchResponse internalSearch(List<String> indices, String[] types, String httpRequest)
			throws ElefanaException {
		final long startTime = System.currentTimeMillis();
		final RequestBodySearch requestBodySearch = new RequestBodySearch(httpRequest);
		if (!requestBodySearch.hasAggregations()) {
			return searchWithoutAggregation(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregation(indices, types, requestBodySearch, startTime);
	}

	private SearchResponse searchWithAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchQuery searchQuery = searchQueryBuilder.buildQuery(indices, types, requestBodySearch);
		final List<String> temporaryTablesCreated = searchQuery.getTemporaryTables();

		final SearchResponse result = executeQuery(searchQuery.getQuery(), startTime, requestBodySearch.getSize());
		final Map<String, Object> aggregationsResult = new HashMap<String, Object>();

		requestBodySearch.getAggregations().executeSqlQuery(indices, types, jdbcTemplate, nodeSettingsService,
				indexFieldMappingService, aggregationsResult, temporaryTablesCreated, searchQuery.getResultTable(),
				requestBodySearch);
		result.setAggregations(aggregationsResult);
		return result;
	}

	private SearchResponse searchWithoutAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchQuery searchQuery = searchQueryBuilder.buildQuery(indices, types, requestBodySearch);
		final List<String> temporaryTablesCreated = searchQuery.getTemporaryTables();

		return executeQuery(searchQuery.getQuery(), startTime, requestBodySearch.getSize());
	}

	private SearchResponse executeQuery(String query, long startTime, int size) throws ElefanaException {
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

	private SearchResponse convertSqlQueryResultToSearchResult(SqlRowSet rowSet, long startTime, int size)
			throws ElefanaException {
		final SearchResponse result = new SearchResponse();
		
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);

		if (rowSet == null) {
			result.getHits().setTotal(0);
		} else if (size > 0) {
			int count = 0;
			while (rowSet.next() && count < size) {
				SearchHit searchHit = new SearchHit();
				searchHit._index = rowSet.getString("_index");
				searchHit._type = rowSet.getString("_type");
				searchHit._id = rowSet.getString("_id");
				searchHit._score = 1.0;
				searchHit._source = JsonIterator.deserialize(rowSet.getString("_source"), new TypeLiteral<Map<String, Object>>(){});
				result.getHits().getHits().add(searchHit);
				count++;
			}
			result.getHits().setTotal(count);
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
			result.getHits().setTotal(totalHits);
		}
		result.getHits().setMaxScore(1.0);

		final long took = System.currentTimeMillis() - startTime;
		result.setTook(took);
		result.setTimedOut(false);

		searchHits.update(result.getHits().getTotal());
		searchTime.update(took);
		return result;
	}

	private SearchResponse getEmptySearchResult(long startTime, int size) throws ElefanaException {
		return convertSqlQueryResultToSearchResult(null, startTime, size);
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(String requestBody) {
		return new PsqlMultiSearchRequest(this, requestBody);
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(String fallbackIndex, String requestBody) {
		MultiSearchRequest result = new PsqlMultiSearchRequest(this, requestBody);
		result.setFallbackIndex(fallbackIndex);
		return result;
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(String fallbackIndex, String fallbackType, String requestBody) {
		MultiSearchRequest result = new PsqlMultiSearchRequest(this, requestBody);
		result.setFallbackIndex(fallbackIndex);
		result.setFallbackType(fallbackType);
		return result;
	}

	@Override
	public SearchRequest prepareSearch(String requestBody) {
		return new PsqlSearchRequest(this, requestBody);
	}

	@Override
	public SearchRequest prepareSearch(String indexPattern, String requestBody) {
		SearchRequest result = new PsqlSearchRequest(this, requestBody);
		result.setIndexPattern(indexPattern);
		return result;
	}

	@Override
	public SearchRequest prepareSearch(String indexPattern, String typesPattern, String requestBody) {
		SearchRequest result = new PsqlSearchRequest(this, requestBody);
		result.setIndexPattern(indexPattern);
		result.setTypePattern(typesPattern);
		return result;
	}
	
	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
