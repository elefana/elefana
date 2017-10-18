/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

import java.util.List;

public interface BucketAggregation extends Aggregation {
	
	public List<Aggregation> getSubAggregations();
}
