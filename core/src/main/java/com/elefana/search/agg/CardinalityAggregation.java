/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.search.agg;

import com.jsoniter.any.Any;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CardinalityAggregation extends Aggregation {
	private static final String KEY_FIELD = "field";
	
	private final String aggregationName;
	private final String fieldName;
	
	public CardinalityAggregation(String aggregationName, Any context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).toString();
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT COUNT(DISTINCT _source->>'" + fieldName + "') AS ");
		queryBuilder.append(aggregationExec.getAggregation().getAggregationName());
		queryBuilder.append(" FROM ");
		queryBuilder.append(aggregationExec.getQueryComponents().getFromComponent());
		if (!aggregationExec.getNodeSettingsService().isUsingCitus()) {
			aggregationExec.getQueryComponents().appendWhere(queryBuilder);
		} else {
			queryBuilder.append(" AS ");
			queryBuilder.append("hit_results");
		}
		
		List<Map<String, Object>> resultSet = aggregationExec.getJdbcTemplate()
				.queryForList(queryBuilder.toString());

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
