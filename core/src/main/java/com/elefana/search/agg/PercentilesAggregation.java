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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class PercentilesAggregation extends Aggregation {
	private static final String KEY_FIELD = "field";
	private static final String KEY_PERCENTS = "percents";
	
	public static final double [] DEFAULT_PERCENTS = new double [] { 1, 5, 25, 50, 75, 95, 99 };
	
	private final String aggregationName;
	private final String fieldName;
	private final double [] percents;
	
	public PercentilesAggregation(String aggregationName, Any context) {
		super();
		this.aggregationName = aggregationName;
		this.fieldName = context.get(KEY_FIELD).toString();
		
		Any percentsContext = context.get(KEY_PERCENTS);
		if(percentsContext.valueType().equals(ValueType.ARRAY)) {
			percents = new double[percentsContext.size()];
			for(int i = 0; i < percentsContext.size(); i++) {
				percents[i] = percentsContext.get(i).toDouble();
			}
		} else {
			percents = DEFAULT_PERCENTS;
		}
	}

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) {
		Map<String, Object> values = new LinkedHashMap<String, Object>();
		for(int i = 0; i < percents.length; i++) {
			values.put(String.valueOf(percents[i]), getPercentile(aggregationExec, fieldName, percents[i]));
		}
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("values", values);

		aggregationExec.getAggregationsResult().put(aggregationExec.getAggregation().getAggregationName(), result);
	}
	
	public static Object getPercentile(AggregationExec aggregationExec, String fieldName, double percentile) {
		final StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("SELECT percentile_disc(");
		queryBuilder.append(String.format("%1$f", percentile / 100f));
		queryBuilder.append(") WITHIN GROUP (ORDER BY (_source->>'" + fieldName + "')::numeric) AS ");
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
		return resultSet.get(0).get(aggregationExec.getAggregation().getAggregationName());
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
