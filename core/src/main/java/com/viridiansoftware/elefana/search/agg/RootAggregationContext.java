/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.elefana.indices.IndexFieldMappingService;
import com.viridiansoftware.elefana.search.RequestBodySearch;

public class RootAggregationContext extends BucketAggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(RootAggregationContext.class);

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
	public void executeSqlQuery(List<String> tableNames, String[] types, JdbcTemplate jdbcTemplate,
			IndexFieldMappingService indexFieldMappingService, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch) {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(tableNames, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
					tempTablesCreated, queryTable, requestBodySearch);
		}
	}

	@Override
	public String getAggregationName() {
		return AggregationsParser.FIELD_AGGS;
	}

	public void setSubAggregations(List<Aggregation> subAggregations) {
		if(subAggregations == null) {
			return;
		}
		this.subaggregations.clear();
		this.subaggregations.addAll(subAggregations);
	}

}
