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

import com.elefana.exception.UnsupportedAggregationTypeException;

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
