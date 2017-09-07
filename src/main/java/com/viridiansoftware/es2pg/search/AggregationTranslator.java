/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pg.search.agg.Aggregation;
import com.viridiansoftware.es2pg.search.agg.AggregationExec;
import com.viridiansoftware.es2pg.search.agg.BucketAggregation;

public class AggregationTranslator extends BucketAggregation {

	public AggregationTranslator() {
		super("aggs");
	}
	
	public void executeSqlQuery(final AggregationExec parentExec, final Map<String, Object> aggregationsResult, final String queryTable) {
		for(Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(parentExec, aggregationsResult, queryTable);
		}
	}

	public void executeSqlQuery(final JdbcTemplate jdbcTemplate, final Map<String, Object> aggregationsResult,
			final List<String> tempTablesCreated, final String queryTable, final RequestBodySearch requestBodySearch) {
		for(Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(jdbcTemplate, aggregationsResult, tempTablesCreated, queryTable, requestBodySearch);
		}
	}
	
	@Override
	protected boolean isAggregationParsed() {
		return true;
	}
}
