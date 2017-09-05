/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search;

import com.viridiansoftware.es2pg.search.agg.BucketAggregation;

public class AggregationTranslator extends BucketAggregation {

	public AggregationTranslator() {
		super("aggs");
	}
	
	@Override
	protected boolean isAggregationParsed() {
		return true;
	}
}
