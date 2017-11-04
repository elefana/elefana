/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.search.agg;

import com.viridiansoftware.elefana.exception.UnsupportedAggregationTypeException;

public enum AggregationType {
	/* METRICS AGGREGATIONS */
	AVG,
	CARDINALITY,
	EXTENDED_STATS,
	GEO_BOUNDS,
	GEO_CENTROID,
	MAX,
	MIN,
	PERCENTILES,
	PERCENTILE_RANKS,
	SCRIPTED_METRIC,
	STATS,
	SUM,
	TOP_HITS,
	VALUE_COUNT,
	
	/* BUCKET AGGREGATIONS */
	ADJACENCY_MATRIX,
	CHILDREN,
	DATE_HISTOGRAM,
	DATE_RANGE,
	DIVERSIFIED_SAMPLER,
	FILTER,
	FILTERS,
	GEO_DISTANCE,
	GEOHASH_GRID,
	GLOBAL,
	HISTOGRAM,
	IP_RANGE,
	MISSING,
	NESTED,
	RANGE,
	REVERSE_NESTED,
	SAMPLER,
	SIGNIFICANT_TERMS,
	TERMS;
	
	public static AggregationType parse(String value) {
		try {
			return AggregationType.valueOf(value.toUpperCase());
		} catch (Exception e) {
			throw new UnsupportedAggregationTypeException();
		}
	}
}
