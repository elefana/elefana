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
package com.elefana.search.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.search.SearchResponse;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.PsqlQueryComponents;
import com.elefana.search.RequestBodySearch;

public abstract class Aggregation {
	public static final String AGGREGATION_TABLE_PREFIX = "elefana_agg_";
	public static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));
	private static final Logger LOGGER = LoggerFactory.getLogger(Aggregation.class);

	public abstract void executeSqlQuery(final AggregationExec aggregationExec) throws ElefanaException;

	protected static void appendIndicesWhereClause(final AggregationExec aggregationExec,
			final StringBuilder queryBuilder) {
		if (aggregationExec.getNodeSettingsService().isUsingCitus()) {
			queryBuilder.append(" WHERE TRUE");
			return;
		}

		queryBuilder.append(" WHERE _index IN (");
		for (int i = 0; i < aggregationExec.getIndices().size(); i++) {
			if (i > 0) {
				queryBuilder.append(',');
			}
			queryBuilder.append("'");
			queryBuilder.append(aggregationExec.getIndices().get(i));
			queryBuilder.append("'");
		}
		queryBuilder.append(")");
	}

	public void executeSqlQuery(AggregationExec parentExec, PsqlQueryComponents queryComponents,
			SearchResponse searchResponse, Map<String, Object> aggregationsResult) throws ElefanaException {
		parentExec.getQueryFutures().offer(parentExec.getExecutorService().submit(new Callable<SearchResponse>() {

			@Override
			public SearchResponse call() throws Exception {
				executeSqlQuery(new AggregationExec(parentExec.getExecutorService(), parentExec.getQueryFutures(),
						parentExec.getIndexTemplate(), parentExec.getIndices(), parentExec.getTypes(),
						parentExec.getJdbcTemplate(), parentExec.getNodeSettingsService(),
						parentExec.getIndexFieldMappingService(), queryComponents, searchResponse, aggregationsResult,
						parentExec.getRequestBodySearch(), Aggregation.this));
				return searchResponse;
			}

		}));
	}

	public void executeSqlQuery(ExecutorService executorService, Queue<Future<SearchResponse>> queryFutures,
			IndexTemplate indexTemplate, List<String> indices, String[] types, JdbcTemplate jdbcTemplate,
			NodeSettingsService nodeSettingsService, PsqlIndexFieldMappingService indexFieldMappingService,
			PsqlQueryComponents queryComponents, SearchResponse searchResponse, Map<String, Object> aggregationsResult,
			RequestBodySearch requestBodySearch) throws ElefanaException {
		queryFutures.offer(executorService.submit(new Callable<SearchResponse>() {

			@Override
			public SearchResponse call() throws Exception {
				executeSqlQuery(new AggregationExec(executorService, queryFutures, indexTemplate, indices, types,
						jdbcTemplate, nodeSettingsService, indexFieldMappingService, queryComponents, searchResponse,
						aggregationsResult, requestBodySearch, Aggregation.this));
				return searchResponse;
			}

		}));
	}

	public abstract String getAggregationName();

	public abstract List<Aggregation> getSubAggregations();
}
