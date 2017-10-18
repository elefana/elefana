/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;

public interface Aggregation {

	public void executeSqlQuery(final AggregationExec parentExec, final Map<String, Object> aggregationsResult,
			final String queryTable);

	public void executeSqlQuery(final List<String> indices, final String[] types, final JdbcTemplate jdbcTemplate,
			final IndexFieldMappingService indexFieldMappingService, final Map<String, Object> aggregationsResult,
			final List<String> tempTablesCreated, final String queryTable, final RequestBodySearch requestBodySearch) ;

	public String getAggregationName();
	
	public boolean isContextFinished();

	public List<Aggregation> getSubAggregations();
}
