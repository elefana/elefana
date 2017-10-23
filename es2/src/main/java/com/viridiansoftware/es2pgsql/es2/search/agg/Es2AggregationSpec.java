/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.agg;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregator;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregator;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregator;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregator;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregator;

import com.viridiansoftware.es2pgsql.exception.UnsupportedAggregationTypeException;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationSpec;

public abstract class Es2AggregationSpec implements AggregationSpec, Aggregation {

	public static Es2AggregationSpec parseAggregation(Aggregator aggregator) {
		if(aggregator instanceof AvgAggregator) {
			
		} else if(aggregator instanceof HistogramAggregator) {
			
		} else if(aggregator instanceof MaxAggregator) {
			
		} else if(aggregator instanceof MinAggregator) {
			
		} else if(aggregator instanceof RangeAggregator) {
			
		} else if(aggregator instanceof StatsAggregator) {
			
		} else if(aggregator instanceof SumAggregator) {
			
		} else if(aggregator instanceof ValueCountAggregator) {
			
		} 
		throw new UnsupportedAggregationTypeException();
	}
}
