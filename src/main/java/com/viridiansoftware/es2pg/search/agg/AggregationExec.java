/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pg.search.RequestBodySearch;

public class AggregationExec {
	private final JdbcTemplate jdbcTemplate;
	private final Map<String, Object> aggregationsResult;
	private final List<String> tempTablesCreated;
	private final String queryTable;
	private final RequestBodySearch requestBodySearch;
	private final Aggregation aggregation;
	
	public AggregationExec(JdbcTemplate jdbcTemplate, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch,
			Aggregation aggregation) {
		super();
		this.jdbcTemplate = jdbcTemplate;
		this.aggregationsResult = aggregationsResult;
		this.tempTablesCreated = tempTablesCreated;
		this.queryTable = queryTable;
		this.requestBodySearch = requestBodySearch;
		this.aggregation = aggregation;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
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
