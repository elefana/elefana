/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

import java.util.ArrayList;
import java.util.List;

public abstract class BucketAggregation extends Aggregation {
	protected final List<Aggregation> subaggregations = new ArrayList<Aggregation>();
	
	@Override
	public List<Aggregation> getSubAggregations() {
		return subaggregations;
	}
}
