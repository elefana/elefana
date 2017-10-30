/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jsoniter.any.Any;

public class MaxAggregation extends Aggregation {
	private static final String KEY_FIELD = "field";
	
	private final String aggregationName;
	private final String fieldName;
	
	public MaxAggregation(String aggregationName, Any context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).toString();
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		List<Map<String, Object>> resultSet = aggregationExec.getJdbcTemplate()
				.queryForList("SELECT max((_source->>'" + fieldName + "')::numeric) AS "
						+ aggregationExec.getAggregation().getAggregationName() + " FROM "
						+ aggregationExec.getQueryTable());

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("value", resultSet.get(0).get(aggregationExec.getAggregation().getAggregationName()));

		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}

	@Override
	public String getAggregationName() {
		return aggregationName;
	}
	
	@Override
	public List<Aggregation> getSubAggregations() {
		return EMPTY_AGGREGATION_LIST;
	}
}
