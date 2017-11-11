/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.elefana.indices.IndexFieldMappingService;
import com.viridiansoftware.elefana.search.RequestBodySearch;

public abstract class Aggregation {
	public static final String AGGREGATION_TABLE_PREFIX = "es2pgsql_agg_";
	public static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));

	public abstract void executeSqlQuery(final AggregationExec aggregationExec);

	public void executeSqlQuery(AggregationExec parentExec, Map<String, Object> aggregationsResult, String queryTable) {
		executeSqlQuery(new AggregationExec(parentExec.getTableNames(), parentExec.getTypes(),
				parentExec.getJdbcTemplate(), parentExec.getIndexFieldMappingService(), aggregationsResult,
				parentExec.getTempTablesCreated(), queryTable, parentExec.getRequestBodySearch(), this));
	}

	public void executeSqlQuery(List<String> tableNames, String[] types, JdbcTemplate jdbcTemplate,
			IndexFieldMappingService indexFieldMappingService, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch) {
		executeSqlQuery(new AggregationExec(tableNames, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
				tempTablesCreated, queryTable, requestBodySearch, this));
	}

	public abstract String getAggregationName();

	public abstract List<Aggregation> getSubAggregations();
}
