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

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.search.SearchResponse;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.PsqlQueryComponents;
import com.elefana.search.RequestBodySearch;

public class AggregationExec {
	private final IndexTemplate indexTemplate;
	private final List<String> indices;
	private final String[] types;
	private final JdbcTemplate jdbcTemplate;
	private final NodeSettingsService nodeSettingsService;
	private final PsqlIndexFieldMappingService indexFieldMappingService;
	private final PsqlQueryComponents queryComponents;
	private final SearchResponse searchResponse;
	private final Map<String, Object> aggregationsResult;
	private final RequestBodySearch requestBodySearch;
	private final Aggregation aggregation;

	public AggregationExec(IndexTemplate indexTemplate, List<String> indices, String[] types, JdbcTemplate jdbcTemplate,
			NodeSettingsService nodeSettingsService, PsqlIndexFieldMappingService indexFieldMappingService,
			PsqlQueryComponents queryComponents, SearchResponse searchResponse, Map<String, Object> aggregationsResult,
			RequestBodySearch requestBodySearch, Aggregation aggregation) {
		super();
		this.indexTemplate = indexTemplate;
		this.indices = indices;
		this.types = types;
		this.jdbcTemplate = jdbcTemplate;
		this.nodeSettingsService = nodeSettingsService;
		this.indexFieldMappingService = indexFieldMappingService;
		this.searchResponse = searchResponse;
		this.queryComponents = queryComponents;
		this.aggregationsResult = aggregationsResult;
		this.requestBodySearch = requestBodySearch;
		this.aggregation = aggregation;
	}

	public IndexTemplate getIndexTemplate() {
		return indexTemplate;
	}

	public List<String> getIndices() {
		return indices;
	}

	public String[] getTypes() {
		return types;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public NodeSettingsService getNodeSettingsService() {
		return nodeSettingsService;
	}

	public PsqlIndexFieldMappingService getIndexFieldMappingService() {
		return indexFieldMappingService;
	}

	public SearchResponse getSearchResponse() {
		return searchResponse;
	}

	public PsqlQueryComponents getQueryComponents() {
		return queryComponents;
	}

	public Map<String, Object> getAggregationsResult() {
		return aggregationsResult;
	}

	public RequestBodySearch getRequestBodySearch() {
		return requestBodySearch;
	}

	public Aggregation getAggregation() {
		return aggregation;
	}
}
