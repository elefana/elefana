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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.UnsupportedAggregationTypeException;
import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AggregationsParser {
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationsParser.class);
	
	public static final String FIELD_AGGS = "aggs";
	public static final String FIELD_AGGREGATIONS = "aggregations";
	
	private static final String AGGREGATION_AVG = "avg";
	private static final String AGGREGATION_CARDINALITY = "cardinality";
	private static final String AGGREGATION_MIN = "min";
	private static final String AGGREGATION_MAX = "max";
	private static final String AGGREGATION_PERCENTILES = "percentiles";
	private static final String AGGREGATION_STATS = "stats";
	private static final String AGGREGATION_SUM = "sum";
	private static final String AGGREGATION_VALUE_COUNT = "value_count";
	
	private static final String AGGREGATION_DATE_HISTOGRAM = "date_histogram";
	private static final String AGGREGATION_RANGE = "range";

	public static List<Aggregation> parseAggregations(String content) throws ElefanaException {
		JsonNode contentContext = JsonUtils.extractJsonNode(content);
		if(contentContext.has(FIELD_AGGS)) {
			return parseAggregations(contentContext.get(FIELD_AGGS));
		} else if(contentContext.has(FIELD_AGGREGATIONS)) {
			return parseAggregations(contentContext.get(FIELD_AGGREGATIONS));
		}
		return null;
	}
	
	public static List<Aggregation> parseAggregations(JsonNode context) throws ElefanaException {
		List<Aggregation> result = new ArrayList<Aggregation>();

		final Iterator<String> keyIterator = context.fieldNames();
		while(keyIterator.hasNext()) {
			final String aggregationName = keyIterator.next();
			final JsonNode aggregationContext = context.get(aggregationName);
			final Aggregation aggregation = parseAggregation(aggregationName, aggregationContext);

			if(aggregationContext.has(FIELD_AGGS)) {
				aggregation.getSubAggregations().addAll(parseAggregations(aggregationContext.get(FIELD_AGGS)));
			} else if(aggregationContext.has(FIELD_AGGREGATIONS)) {
				aggregation.getSubAggregations().addAll(parseAggregations(aggregationContext.get(FIELD_AGGREGATIONS)));
			}
			result.add(aggregation);
		}
		return result;
	}
	
	public static Aggregation parseAggregation(String aggregationName, JsonNode context) throws ElefanaException {
		if(context.has(AGGREGATION_AVG)) {
			return new AvgAggregation(aggregationName, context.get(AGGREGATION_AVG));
		}
		if(context.has(AGGREGATION_CARDINALITY)) {
			return new CardinalityAggregation(aggregationName, context.get(AGGREGATION_CARDINALITY));
		}
		if(context.has(AGGREGATION_MIN)) {
			return new MinAggregation(aggregationName, context.get(AGGREGATION_MIN));
		}
		if(context.has(AGGREGATION_MAX)) {
			return new MaxAggregation(aggregationName, context.get(AGGREGATION_MAX));
		}
		if(context.has(AGGREGATION_PERCENTILES)) {
			return new PercentilesAggregation(aggregationName, context.get(AGGREGATION_PERCENTILES));
		}
		if(context.has(AGGREGATION_STATS)) {
			return new StatsAggregation(aggregationName, context.get(AGGREGATION_STATS));
		}
		if(context.has(AGGREGATION_SUM)) {
			return new SumAggregation(aggregationName, context.get(AGGREGATION_SUM));
		}
		if(context.has(AGGREGATION_VALUE_COUNT)) {
			return new ValueCountAggregation(aggregationName, context.get(AGGREGATION_VALUE_COUNT));
		}
		if(context.has(AGGREGATION_RANGE)) {
			return new RangeAggregation(aggregationName, context.get(AGGREGATION_RANGE));
		}
		if(context.has(AGGREGATION_DATE_HISTOGRAM)) {
			return new DateHistogramAggregation(aggregationName, context.get(AGGREGATION_DATE_HISTOGRAM));
		}
		throw new UnsupportedAggregationTypeException(aggregationName);
	}
}
