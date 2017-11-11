/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.jsoniter.any.Any;
import com.viridiansoftware.elefana.indices.IndexFieldMappingService;
import com.viridiansoftware.elefana.search.RequestBodySearch;

public class SumAggregation extends Aggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(SumAggregation.class);
	
	private static final String KEY_FIELD = "field";
	
	private final String aggregationName;
	private final String fieldName;
	
	public SumAggregation(String aggregationName, Any context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).toString();
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		String query = "SELECT sum((_source->>'" + fieldName + "')::numeric) AS "
				+ aggregationExec.getAggregation().getAggregationName() + " FROM "
				+ aggregationExec.getQueryTable();
		LOGGER.info(query);
		
		List<Map<String, Object>> resultSet = aggregationExec.getJdbcTemplate()
				.queryForList(query);

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
