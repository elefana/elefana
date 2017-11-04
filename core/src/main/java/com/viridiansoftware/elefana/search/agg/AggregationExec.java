/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.elefana.indices.IndexFieldMappingService;
import com.viridiansoftware.elefana.search.RequestBodySearch;

public class AggregationExec {
	private final List<String> indices;
	private final String [] types;
	private final JdbcTemplate jdbcTemplate;
	private final IndexFieldMappingService indexFieldMappingService;
	private final Map<String, Object> aggregationsResult;
	private final List<String> tempTablesCreated;
	private final String queryTable;
	private final RequestBodySearch requestBodySearch;
	private final Aggregation aggregation;

	public AggregationExec(List<String> indices, String [] types, JdbcTemplate jdbcTemplate, IndexFieldMappingService indexFieldMappingService,
			Map<String, Object> aggregationsResult, List<String> tempTablesCreated, String queryTable,
			RequestBodySearch requestBodySearch, Aggregation aggregation) {
		super();
		this.indices = indices;
		this.types = types;
		this.jdbcTemplate = jdbcTemplate;
		this.indexFieldMappingService = indexFieldMappingService;
		this.aggregationsResult = aggregationsResult;
		this.tempTablesCreated = tempTablesCreated;
		this.queryTable = queryTable;
		this.requestBodySearch = requestBodySearch;
		this.aggregation = aggregation;
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

	public IndexFieldMappingService getIndexFieldMappingService() {
		return indexFieldMappingService;
	}

	public Map<String, Object> getAggregationsResult() {
		return aggregationsResult;
	}

	public List<String> getTempTablesCreated() {
		return tempTablesCreated;
	}

	public String getQueryTable() {
		return queryTable;
	}

	public RequestBodySearch getRequestBodySearch() {
		return requestBodySearch;
	}

	public Aggregation getAggregation() {
		return aggregation;
	}
}
