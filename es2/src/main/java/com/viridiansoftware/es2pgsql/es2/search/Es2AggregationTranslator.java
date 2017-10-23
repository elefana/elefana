/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search;

import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.es2.search.agg.Es2AggregationSpec;
import com.viridiansoftware.es2pgsql.es2.search.agg.Es2BucketAggregation;
import com.viridiansoftware.es2pgsql.search.AggregationTranslator;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class Es2AggregationTranslator extends Es2BucketAggregation implements AggregationTranslator {

	public Es2AggregationTranslator(SearchContextAggregations aggregations) {
		super("aggs");
		for(Aggregator aggregator : aggregations.aggregators()) {
			subaggregations.add(Es2AggregationSpec.parseAggregation(aggregator));
		}
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
}
 