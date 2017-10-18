/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.es5.search.agg.Es5BucketAggregation;
import com.viridiansoftware.es2pgsql.search.AggregationTranslator;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class Es5AggregationTranslator extends Es5BucketAggregation implements AggregationTranslator {

	public Es5AggregationTranslator() {
		super("aggs");
	}

	@Override
	public void executeSqlQuery(final AggregationExec parentExec, final Map<String, Object> aggregationsResult,
			final String queryTable) {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(parentExec, aggregationsResult, queryTable);
		}
	}

	@Override
	public void executeSqlQuery(final List<String> indices, final String[] types, final JdbcTemplate jdbcTemplate,
			final IndexFieldMappingService indexFieldMappingService, final Map<String, Object> aggregationsResult,
			final List<String> tempTablesCreated, final String queryTable, final RequestBodySearch requestBodySearch) {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(indices, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
					tempTablesCreated, queryTable, requestBodySearch);
		}
	}

	@Override
	protected boolean isAggregationParsed() {
		return true;
	}
}
