/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.agg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator;
import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.search.AggregationType;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class AvgAggregationSpec extends Es2AggregationSpec {
	private String fieldName;
	
	public AvgAggregationSpec(AvgAggregator avgAggregator) {
		super();
		//this.fieldName = avgAggregator.
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		List<Map<String, Object>> resultSet = aggregationExec.getJdbcTemplate()
				.queryForList("SELECT avg((_source->>'" + fieldName + "')::numeric) AS "
						+ aggregationExec.getAggregation().getAggregationName() + " FROM "
						+ aggregationExec.getQueryTable());

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("value", resultSet.get(0).get(aggregationExec.getAggregation().getAggregationName()));

		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}

	@Override
	public void executeSqlQuery(AggregationExec parentExec, Map<String, Object> aggregationsResult, String queryTable) {
		executeSqlQuery(new AggregationExec(parentExec.getIndices(), parentExec.getTypes(),
				parentExec.getJdbcTemplate(), parentExec.getIndexFieldMappingService(), aggregationsResult,
				parentExec.getTempTablesCreated(), queryTable, parentExec.getRequestBodySearch(), this));
	}

	@Override
	public void executeSqlQuery(List<String> indices, String[] types, JdbcTemplate jdbcTemplate,
			IndexFieldMappingService indexFieldMappingService, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch) {
		executeSqlQuery(new AggregationExec(indices, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
				tempTablesCreated, queryTable, requestBodySearch, this));
	}

	@Override
	public String getAggregationName() {
		return AggregationType.AVG.name();
	}

	@Override
	public boolean isContextFinished() {
		return true;
	}

	@Override
	public List<Aggregation> getSubAggregations() {
		return Es2Aggregations.EMPTY_AGGREGATION_LIST;
	}
}
