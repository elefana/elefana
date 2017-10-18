/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.agg;

public interface AggregationSpec {
	public static final String AGGREGATION_TABLE_PREFIX = "es2pgsql_agg_";

	public void executeSqlQuery(final AggregationExec aggregationExec);
}