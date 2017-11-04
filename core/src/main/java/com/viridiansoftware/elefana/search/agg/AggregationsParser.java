/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import java.util.ArrayList;
import java.util.List;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.viridiansoftware.elefana.exception.UnsupportedAggregationTypeException;

public class AggregationsParser {
	public static final String FIELD_AGGS = "aggs";
	public static final String FIELD_AGGREGATIONS = "aggregations";
	
	private static final String AGGREGATION_AVG = "avg";
	private static final String AGGREGATION_MIN = "min";
	private static final String AGGREGATION_MAX = "max";
	private static final String AGGREGATION_SUM = "sum";
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
			System.out.println(aggregationContext);
			
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
		if(!context.get(AGGREGATION_MIN).valueType().equals(ValueType.INVALID)) {
			return new MinAggregation(aggregationName, context.get(AGGREGATION_MIN));
		}
		if(!context.get(AGGREGATION_MAX).valueType().equals(ValueType.INVALID)) {
			return new MaxAggregation(aggregationName, context.get(AGGREGATION_MAX));
		}
		if(!context.get(AGGREGATION_SUM).valueType().equals(ValueType.INVALID)) {
			return new SumAggregation(aggregationName, context.get(AGGREGATION_SUM));
		}
		if(!context.get(AGGREGATION_RANGE).valueType().equals(ValueType.INVALID)) {
			return new RangeAggregation(aggregationName, context.get(AGGREGATION_RANGE));
		}
		throw new UnsupportedAggregationTypeException(aggregationName);
	}
}
