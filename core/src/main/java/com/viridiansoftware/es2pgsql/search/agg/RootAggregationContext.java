/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.indices.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;

public class RootAggregationContext extends BucketAggregation {
	private List<Aggregation> subAggregations;

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		for (Aggregation aggregation : getSubAggregations()) {
			aggregation.executeSqlQuery(aggregationExec);
		}
	}
	
	@Override
	public void executeSqlQuery(AggregationExec parentExec, Map<String, Object> aggregationsResult, String queryTable) {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(parentExec, aggregationsResult, queryTable);
		}
	}
	
	@Override
	public void executeSqlQuery(List<String> indices, String[] types, JdbcTemplate jdbcTemplate,
			IndexFieldMappingService indexFieldMappingService, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch) {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(indices, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
					tempTablesCreated, queryTable, requestBodySearch);
		}
	}

	@Override
	public String getAggregationName() {
		return AggregationsParser.FIELD_AGGS;
	}

	@Override
	public List<Aggregation> getSubAggregations() {
		return subAggregations;
	}

	public void setSubAggregations(List<Aggregation> subAggregations) {
		this.subAggregations = subAggregations;
	}

}
