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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.json.JsonUtils;
import com.elefana.api.search.MultiSearchRequest;
import com.elefana.api.search.MultiSearchResponse;
import com.elefana.api.search.SearchRequest;
import com.elefana.api.search.SearchResponse;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.*;
import com.elefana.table.TableGarbageCollector;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

@Service
@DependsOn("nodeSettingsService")
public class PsqlSearchService implements SearchService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlSearchService.class);
	private static final int DEFAULT_FETCH_SIZE = 250;

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
	private IndexFieldStatsService indexFieldStatsService;
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

	private int sqlFetchSize;

	@PostConstruct
	public void postConstruct() {
		sqlFetchSize = environment.getProperty("elefana.service.search.sql.fetchSize", Integer.class, DEFAULT_FETCH_SIZE);

		final int totalRequestThreads = environment.getProperty("elefana.service.search.count.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalHitsThreads = environment.getProperty("elefana.service.search.hits.threads", Integer.class,
				Runtime.getRuntime().availableProcessors());
		final int totalAggregationThreads = environment.getProperty("elefana.service.search.aggregation.threads",
				Integer.class, Runtime.getRuntime().availableProcessors());
		searchCountExecutorService = Executors.newFixedThreadPool(totalRequestThreads, new NamedThreadFactory("elefana-searchService-countExecutor"));
		searchHitsExecutorService = Executors.newFixedThreadPool(totalHitsThreads, new NamedThreadFactory("elefana-searchService-hitsExecutor"));
		searchAggregationsExecutorService = Executors.newFixedThreadPool(totalAggregationThreads, new NamedThreadFactory("elefana-searchService-aggregationsExecutor"));

		searchHitsTime = metricRegistry.histogram(MetricRegistry.name("search", "hits", "time"));
		searchHitsSize = metricRegistry.histogram(MetricRegistry.name("search", "hits", "size"));
		searchAggregationTime = metricRegistry.histogram(MetricRegistry.name("search", "aggregation", "time"));
		searchTotalTime = metricRegistry.histogram(MetricRegistry.name("search", "time"));

		if (nodeSettingsService.isUsingCitus()) {
			searchQueryBuilder = new CitusSearchQueryBuilder(jdbcTemplate, indexUtils, indexFieldStatsService);
			searchHitsQueryExecutor = new CitusSearchHitsQueryExecutor(jdbcTemplate, searchHitsTime, searchHitsSize);
		} else {
			searchQueryBuilder = new PartitionTableSearchQueryBuilder(jdbcTemplate, indexUtils, indexFieldStatsService);
			searchHitsQueryExecutor = new PartitionTableSearchHitsQueryExecutor(jdbcTemplate, searchHitsTime,
					searchHitsSize);
		}
	}

	@PreDestroy
	public void preDestroy() {
		searchCountExecutorService.shutdown();

		try {
			searchCountExecutorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}

	public MultiSearchResponse multiSearch(ChannelHandlerContext context, PooledStringBuilder httpRequest) throws ElefanaException {
		return multiSearch(context,null, null, httpRequest);
	}

	public MultiSearchResponse multiSearch(ChannelHandlerContext context, String fallbackIndex, PooledStringBuilder httpRequest) throws ElefanaException {
		return multiSearch(context, fallbackIndex, null, httpRequest);
	}

	public MultiSearchResponse multiSearch(ChannelHandlerContext context, String fallbackIndex, String fallbackType, PooledStringBuilder httpRequest)
			throws ElefanaException {
		final String request = httpRequest.toString();
		String[] lines = request.split("\n");

		final MultiSearchResponse result = new MultiSearchResponse();

		for (int i = 0; i < lines.length; i += 2) {
			Map<String, Object> indexTypeInfo = JsonUtils.fromJsonString(lines[i], Map.class);
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

	public SearchResponse search(ChannelHandlerContext context, PooledStringBuilder httpRequest) throws ElefanaException {
		return search(context, null, null, httpRequest);
	}

	public SearchResponse search(ChannelHandlerContext context, String indexPattern, PooledStringBuilder httpRequest) throws ElefanaException {
		return search(context, indexPattern, null, httpRequest);
	}

	public SearchResponse search(ChannelHandlerContext context, String indexPattern, String typesPattern, PooledStringBuilder httpRequest) throws ElefanaException {
		List<String> indices = indexPattern == null || indexPattern.isEmpty() ? indexUtils.listIndices()
				: indexUtils.listIndicesForIndexPattern(indexPattern);
		final Set<String> types = new HashSet<String>();
		if (typesPattern != null) {
			for (String index : indices) {
				types.addAll(indexFieldMappingService.getTypesForIndex(index, typesPattern));
			}
		}
		return internalSearch(indices, types.toArray(EMPTY_TYPES_LIST), httpRequest);
	}

	private SearchResponse internalSearch(List<String> indices, String[] types, PooledStringBuilder httpRequest)
			throws ElefanaException {
		final long startTime = System.currentTimeMillis();
		final RequestBodySearch requestBodySearch = new RequestBodySearch(httpRequest.toString());
		if (!requestBodySearch.hasAggregations()) {
			return searchWithoutAggregation(indices, types, requestBodySearch, startTime);
		}
		return searchWithAggregation(indices, types, requestBodySearch, startTime);
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
		final Map<String, Object> aggregationsResult = new ConcurrentHashMap<String, Object>();
		final Queue<Future<SearchResponse>> queryFutures = new ConcurrentLinkedQueue<>();

		Connection countConnection = null;
		Connection hitsConnection = null;

		Statement countStatement = null;
		Statement hitsStatement = null;

		try {
			countConnection = jdbcTemplate.getDataSource().getConnection();
			hitsConnection = jdbcTemplate.getDataSource().getConnection();

			hitsConnection.setAutoCommit(false);

			countStatement = countConnection.createStatement();
			hitsStatement = hitsConnection.createStatement();

			hitsStatement.setFetchSize(sqlFetchSize);

			final Future<SearchResponse> countFuture = executeCountQuery(result, countStatement, queryComponents, startTime,
					requestBodySearch.getFrom(), requestBodySearch.getSize());

			queryFutures.offer(executeHitsQuery(result, hitsStatement, queryComponents, startTime, requestBodySearch.getFrom(),
					requestBodySearch.getSize()));

			countFuture.get();

			disposeStatement(countStatement);
			countStatement = null;
			disposeConnection(countConnection);
			countConnection = null;

			requestBodySearch.getAggregations().executeSqlQuery(searchAggregationsExecutorService, queryFutures,
					indexTemplate, indices, types, jdbcTemplate, nodeSettingsService, indexFieldMappingService,
					queryComponents, result, aggregationsResult, requestBodySearch);
			result.setAggregations(aggregationsResult);

			while (!queryFutures.isEmpty()) {
				queryFutures.poll().get();
			}
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		}

		final long took = System.currentTimeMillis() - startTime;
		searchTotalTime.update(took);
		result.setTook(took);
		result.setTimedOut(false);
		tableGarbageCollector.queueTemporaryTablesForDeletion(temporaryTablesCreated);

		disposeStatement(countStatement);
		disposeConnection(countConnection);

		disposeStatement(hitsStatement);
		disposeConnection(hitsConnection);
		return result;
	}

	private SearchResponse searchWithoutAggregation(List<String> indices, String[] types,
	                                                RequestBodySearch requestBodySearch, long startTime) throws ElefanaException {
		final SearchResponse result = new SearchResponse();
		final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndices(indices);
		final PsqlQueryComponents queryComponents = searchQueryBuilder.buildQuery(indexTemplate, indices, types,
				requestBodySearch);

		Connection countConnection = null;
		Connection hitsConnection = null;

		Statement countStatement = null;
		Statement hitsStatement = null;

		try {
			countConnection = jdbcTemplate.getDataSource().getConnection();
			hitsConnection = jdbcTemplate.getDataSource().getConnection();

			hitsConnection.setAutoCommit(false);

			countStatement = countConnection.createStatement();
			hitsStatement = hitsConnection.createStatement();

			hitsStatement.setFetchSize(sqlFetchSize);

			final Future<SearchResponse> countQueryFuture = executeCountQuery(result, countStatement, queryComponents, startTime,
					requestBodySearch.getFrom(), requestBodySearch.getSize());
			final Future<SearchResponse> hitsQueryFuture = executeHitsQuery(result, hitsStatement, queryComponents, startTime,
					requestBodySearch.getFrom(), requestBodySearch.getSize());

			countQueryFuture.get();
			hitsQueryFuture.get();
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);

			disposeStatement(countStatement);
			disposeConnection(countConnection);

			disposeStatement(hitsStatement);
			disposeConnection(hitsConnection);

			throw new ShardFailedException(e);
		}

		final long took = System.currentTimeMillis() - startTime;
		searchTotalTime.update(took);
		result.setTook(took);
		result.setTimedOut(false);

		disposeStatement(countStatement);
		disposeConnection(countConnection);

		disposeStatement(hitsStatement);
		disposeConnection(hitsConnection);
		return result;
	}

	private Future<SearchResponse> executeCountQuery(SearchResponse response, Statement statement, PsqlQueryComponents queryComponents,
	                                                 long startTime, int from, int size) throws ElefanaException {
		return searchHitsExecutorService
				.submit(searchHitsQueryExecutor.executeCountQuery(response, statement,
						queryComponents, startTime, from, size));
	}

	private Future<SearchResponse> executeHitsQuery(SearchResponse response, Statement statement, PsqlQueryComponents queryComponents,
	                                                long startTime, int from, int size) throws ElefanaException {
		return searchHitsExecutorService
				.submit(searchHitsQueryExecutor.executeHitsQuery(response, statement,
						queryComponents, startTime, from, size));
	}

	private void disposeStatement(Statement statement) {
		if(statement == null) {
			return;
		}
		try {
			statement.close();
		} catch (SQLException e) {}
	}

	private void disposeConnection(Connection connection) {
		if(connection == null) {
			return;
		}
		try {
			connection.setAutoCommit(true);
			connection.close();
		} catch (SQLException e) {}
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, PooledStringBuilder requestBody) {
		return new PsqlMultiSearchRequest(this, context, requestBody);
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, String fallbackIndex, PooledStringBuilder requestBody) {
		MultiSearchRequest result = new PsqlMultiSearchRequest(this, context, requestBody);
		result.setFallbackIndex(fallbackIndex);
		return result;
	}

	@Override
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, String fallbackIndex, String fallbackType, PooledStringBuilder requestBody) {
		MultiSearchRequest result = new PsqlMultiSearchRequest(this, context, requestBody);
		result.setFallbackIndex(fallbackIndex);
		result.setFallbackType(fallbackType);
		return result;
	}

	@Override
	public SearchRequest prepareSearch(ChannelHandlerContext context, PooledStringBuilder requestBody) {
		return new PsqlSearchRequest(this, context, requestBody);
	}

	@Override
	public SearchRequest prepareSearch(ChannelHandlerContext context, String indexPattern, PooledStringBuilder requestBody) {
		SearchRequest result = new PsqlSearchRequest(this, context, requestBody);
		result.setIndexPattern(indexPattern);
		return result;
	}

	@Override
	public SearchRequest prepareSearch(ChannelHandlerContext context, String indexPattern, String typesPattern, PooledStringBuilder requestBody) {
		SearchRequest result = new PsqlSearchRequest(this, context, requestBody);
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
