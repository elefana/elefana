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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.search.MultiSearchRequest;
import com.elefana.api.search.MultiSearchResponse;
import com.elefana.api.search.SearchRequest;
import com.elefana.api.search.SearchResponse;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.CitusSearchHitsQueryExecutor;
import com.elefana.search.CitusSearchQueryBuilder;
import com.elefana.search.PartitionTableSearchHitsQueryExecutor;
import com.elefana.search.PartitionTableSearchQueryBuilder;
import com.elefana.search.PsqlQueryComponents;
import com.elefana.search.RequestBodySearch;
import com.elefana.search.SearchHitsQueryExecutor;
import com.elefana.search.SearchQueryBuilder;
import com.elefana.search.SearchService;
import com.elefana.util.IndexUtils;
import com.elefana.util.TableGarbageCollector;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.TypeLiteral;

@Service
@DependsOn("nodeSettingsService")
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
	private PsqlIndexTemplateService indexTemplateService;
	@Autowired
	private TableGarbageCollector tableGarbageCollector;
	@Autowired
	private IndexUtils indexUtils;
	@Autowired
	private MetricRegistry metricRegistry;

	private ExecutorService searchCountExecutorService;
	private ExecutorService searchHitsExecutorService;
	private ExecutorService searchAggregationsExecutorService;
	private SearchQueryBuilder searchQueryBuilder;
	private SearchHitsQueryExecutor searchHitsQueryExecutor;
	private Histogram searchHitsTime, searchHitsSize, searchAggregationTime, searchTotalTime;

	@PostConstruct
	public void postConstruct() {
		final int totalRequestThreads = environment.getProperty("elefana.service.search.count.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalHitsThreads = environment.getProperty("elefana.service.search.hits.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalAggregationThreads = environment.getProperty("elefana.service.search.aggregation.threads",
				Integer.class, Runtime.getRuntime().availableProcessors());
		searchCountExecutorService = Executors.newFixedThreadPool(totalRequestThreads);
		searchHitsExecutorService = Executors.newFixedThreadPool(totalHitsThreads);
		searchAggregationsExecutorService = Executors.newFixedThreadPool(totalAggregationThreads);

		searchHitsTime = metricRegistry.histogram(MetricRegistry.name("search", "hits", "time"));
		searchHitsSize = metricRegistry.histogram(MetricRegistry.name("search", "hits", "size"));
		searchAggregationTime = metricRegistry.histogram(MetricRegistry.name("search", "aggregation", "time"));
		searchTotalTime = metricRegistry.histogram(MetricRegistry.name("search", "time"));

		if (nodeSettingsService.isUsingCitus()) {
			searchQueryBuilder = new CitusSearchQueryBuilder(jdbcTemplate, indexUtils);
			searchHitsQueryExecutor = new CitusSearchHitsQueryExecutor(jdbcTemplate, searchHitsTime, searchHitsSize);
		} else {
			searchQueryBuilder = new PartitionTableSearchQueryBuilder(jdbcTemplate, indexUtils);
			searchHitsQueryExecutor = new PartitionTableSearchHitsQueryExecutor(jdbcTemplate, searchHitsTime,
					searchHitsSize);
		}
	}

	@PreDestroy
	public void preDestroy() {
		searchCountExecutorService.shutdown();
	}

	public MultiSearchResponse multiSearch(String httpRequest) throws ElefanaException {
		return multiSearch(null, null, httpRequest);
	}

	public MultiSearchResponse multiSearch(String fallbackIndex, String httpRequest) throws ElefanaException {
		return multiSearch(fallbackIndex, null, httpRequest);
	}

	public MultiSearchResponse multiSearch(String fallbackIndex, String fallbackType, String httpRequest)
			throws ElefanaException {
		String[] lines = httpRequest.split("\n");

		final MultiSearchResponse result = new MultiSearchResponse();

		for (int i = 0; i < lines.length; i += 2) {
			Map<String, Object> indexTypeInfo = JsonIterator.deserialize(lines[i],
					new TypeLiteral<Map<String, Object>>() {
					});
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
		return search(indexPattern, null, httpRequest);
	}

	public SearchResponse search(String indexPattern, String typesPattern, String httpRequest) throws ElefanaException {
		List<String> indices = indexPattern == null || indexPattern.isEmpty() ? indexUtils.listIndices()
				: indexUtils.listIndicesForIndexPattern(indexPattern);
		final Set<String> types = new HashSet<String>();
		if(typesPattern != null) {
			for(String index : indices) {
				types.addAll(indexFieldMappingService.getTypesForIndex(index, typesPattern));
			}
		}
		return internalSearch(indices, types.toArray(EMPTY_TYPES_LIST), httpRequest);
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
		final SearchResponse result = new SearchResponse();
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndices(indices);
		final PsqlQueryComponents queryComponents = searchQueryBuilder.buildQuery(indexTemplate, indices, types,
				requestBodySearch);
		final List<String> temporaryTablesCreated = queryComponents.getTemporaryTables();

		final Queue<Future<SearchResponse>> queryFutures = new ConcurrentLinkedQueue<>();
		final Future<SearchResponse> countFuture = executeCountQuery(result, queryComponents, startTime,
				requestBodySearch.getFrom(), requestBodySearch.getSize());

		queryFutures.offer(executeHitsQuery(result, queryComponents, startTime, requestBodySearch.getFrom(),
				requestBodySearch.getSize()));
		final Map<String, Object> aggregationsResult = new ConcurrentHashMap<String, Object>();

		try {
			countFuture.get();
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		}

		requestBodySearch.getAggregations().executeSqlQuery(searchAggregationsExecutorService, queryFutures,
				indexTemplate, indices, types, jdbcTemplate, nodeSettingsService, indexFieldMappingService,
				queryComponents, result, aggregationsResult, requestBodySearch);
		result.setAggregations(aggregationsResult);

		try {
			while (!queryFutures.isEmpty()) {
				queryFutures.poll().get();
			}
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		}
		final long took = System.currentTimeMillis() - startTime;
		searchTotalTime.update(took);
		result.setTook(took);
		result.setTimedOut(false);
		tableGarbageCollector.queueTemporaryTablesForDeletion(temporaryTablesCreated);
		return result;
	}

	private SearchResponse searchWithoutAggregation(List<String> indices, String[] types,
			RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchResponse result = new SearchResponse();
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndices(indices);
		final PsqlQueryComponents queryComponents = searchQueryBuilder.buildQuery(indexTemplate, indices, types,
				requestBodySearch);
		final Future<SearchResponse> countQueryFuture = executeCountQuery(result, queryComponents, startTime,
				requestBodySearch.getFrom(), requestBodySearch.getSize());
		final Future<SearchResponse> hitsQueryFuture = executeHitsQuery(result, queryComponents, startTime,
				requestBodySearch.getFrom(), requestBodySearch.getSize());
		try {
			countQueryFuture.get();
			hitsQueryFuture.get();
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
			throw new ShardFailedException(e);
		}
		final long took = System.currentTimeMillis() - startTime;
		searchTotalTime.update(took);
		result.setTook(took);
		result.setTimedOut(false);
		return result;
	}

	private Future<SearchResponse> executeCountQuery(SearchResponse response, PsqlQueryComponents queryComponents,
			long startTime, int from, int size) throws ElefanaException {
		return searchHitsExecutorService
				.submit(searchHitsQueryExecutor.executeCountQuery(response, queryComponents, startTime, from, size));
	}

	private Future<SearchResponse> executeHitsQuery(SearchResponse response, PsqlQueryComponents queryComponents,
			long startTime, int from, int size) throws ElefanaException {
		return searchHitsExecutorService
				.submit(searchHitsQueryExecutor.executeHitsQuery(response, queryComponents, startTime, from, size));
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
		return searchCountExecutorService.submit(request);
	}

	private SearchResponse createSearchResponse() {
		final SearchResponse result = new SearchResponse();
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);
		return result;
	}
}
