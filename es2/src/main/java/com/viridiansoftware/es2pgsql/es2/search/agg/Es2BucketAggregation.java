/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.agg;

import java.util.ArrayList;
import java.util.List;

import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.BucketAggregation;

public class Es2BucketAggregation extends Es2Aggregations implements BucketAggregation {
	protected final List<Aggregation> subaggregations = new ArrayList<Aggregation>(1);

	public Es2BucketAggregation(String aggregationName) {
		super(aggregationName);
	}
}
