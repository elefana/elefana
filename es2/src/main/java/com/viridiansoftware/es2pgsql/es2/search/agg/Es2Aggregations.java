/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class Es2Aggregations implements Aggregation {
	public static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));
	
	protected final String aggregationName;
	
	protected Es2AggregationSpec aggregationSpec;
	
	public Es2Aggregations(String aggregationName) {
		super();
		this.aggregationName = aggregationName;
	}

	public void executeSqlQuery(final AggregationExec parentExec, final Map<String, Object> aggregationsResult,
			final String queryTable) {
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.executeSqlQuery(new AggregationExec(parentExec.getIndices(), parentExec.getTypes(),
				parentExec.getJdbcTemplate(), parentExec.getIndexFieldMappingService(), aggregationsResult,
				parentExec.getTempTablesCreated(), queryTable, parentExec.getRequestBodySearch(), this));
	}

	public void executeSqlQuery(final List<String> indices, final String[] types, final JdbcTemplate jdbcTemplate,
			final IndexFieldMappingService indexFieldMappingService, final Map<String, Object> aggregationsResult,
			final List<String> tempTablesCreated, final String queryTable, final RequestBodySearch requestBodySearch) {
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.executeSqlQuery(new AggregationExec(indices, types, jdbcTemplate, indexFieldMappingService,
				aggregationsResult, tempTablesCreated, queryTable, requestBodySearch, this));
	}

	@Override
	public boolean isContextFinished() {
		return true;
	}

	public String getAggregationName() {
		return aggregationName;
	}

	public List<Aggregation> getSubAggregations() {
		return EMPTY_AGGREGATION_LIST;
	}
}
