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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elefana.exception.UnsupportedAggregationTypeException;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class AggregationsParser {
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationsParser.class);
	
	public static final String FIELD_AGGS = "aggs";
	public static final String FIELD_AGGREGATIONS = "aggregations";
	
	private static final String AGGREGATION_AVG = "avg";
	private static final String AGGREGATION_CARDINALITY = "cardinality";
	private static final String AGGREGATION_MIN = "min";
	private static final String AGGREGATION_MAX = "max";
	private static final String AGGREGATION_STATS = "stats";
	private static final String AGGREGATION_SUM = "sum";
	private static final String AGGREGATION_VALUE_COUNT = "value_count";
	
	private static final String AGGREGATION_DATE_HISTOGRAM = "date_histogram";
	private static final String AGGREGATION_RANGE = "range";

	public static List<Aggregation> parseAggregations(String content) {
		Any contentContext = JsonIterator.deserialize(content);
		if(!contentContext.get(FIELD_AGGS).valueType().equals(ValueType.INVALID)) {
			return parseAggregations(contentContext.get(FIELD_AGGS));
		} else if(!contentContext.get(FIELD_AGGREGATIONS).valueType().equals(ValueType.INVALID)) {
			return parseAggregations(contentContext.get(FIELD_AGGREGATIONS));
		}
		return null;
	}
	
	public static List<Aggregation> parseAggregations(Any context) {
		List<Aggregation> result = new ArrayList<Aggregation>();
		
		for(String aggregationName : context.keys()) {
			Any aggregationContext = context.get(aggregationName);
			Aggregation aggregation = parseAggregation(aggregationName, aggregationContext);
			
			if(!aggregationContext.get(FIELD_AGGS).valueType().equals(ValueType.INVALID)) {
				aggregation.getSubAggregations().addAll(parseAggregations(aggregationContext.get(FIELD_AGGS)));
			} else if(!aggregationContext.get(FIELD_AGGREGATIONS).valueType().equals(ValueType.INVALID)) {
				aggregation.getSubAggregations().addAll(parseAggregations(aggregationContext.get(FIELD_AGGREGATIONS)));
			}
			result.add(aggregation);
		}
		return result;
	}
	
	public static Aggregation parseAggregation(String aggregationName, Any context) {
		if(!context.get(AGGREGATION_AVG).valueType().equals(ValueType.INVALID)) {
			return new AvgAggregation(aggregationName, context.get(AGGREGATION_AVG));
		}
		if(!context.get(AGGREGATION_CARDINALITY).valueType().equals(ValueType.INVALID)) {
			return new CardinalityAggregation(aggregationName, context.get(AGGREGATION_CARDINALITY));
		}
		if(!context.get(AGGREGATION_MIN).valueType().equals(ValueType.INVALID)) {
			return new MinAggregation(aggregationName, context.get(AGGREGATION_MIN));
		}
		if(!context.get(AGGREGATION_MAX).valueType().equals(ValueType.INVALID)) {
			return new MaxAggregation(aggregationName, context.get(AGGREGATION_MAX));
		}
		if(!context.get(AGGREGATION_STATS).valueType().equals(ValueType.INVALID)) {
			return new StatsAggregation(aggregationName, context.get(AGGREGATION_STATS));
		}
		if(!context.get(AGGREGATION_SUM).valueType().equals(ValueType.INVALID)) {
			return new SumAggregation(aggregationName, context.get(AGGREGATION_SUM));
		}
		if(!context.get(AGGREGATION_VALUE_COUNT).valueType().equals(ValueType.INVALID)) {
			return new ValueCountAggregation(aggregationName, context.get(AGGREGATION_VALUE_COUNT));
		}
		
		if(!context.get(AGGREGATION_RANGE).valueType().equals(ValueType.INVALID)) {
			return new RangeAggregation(aggregationName, context.get(AGGREGATION_RANGE));
		}
		if(!context.get(AGGREGATION_DATE_HISTOGRAM).valueType().equals(ValueType.INVALID)) {
			return new DateHistogramAggregation(aggregationName, context.get(AGGREGATION_DATE_HISTOGRAM));
		}
		throw new UnsupportedAggregationTypeException(aggregationName);
	}
}
