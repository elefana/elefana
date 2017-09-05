/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.util.List;

import com.viridiansoftware.es2pg.search.EsSearchParseContext;

public abstract class AggregationSpec extends EsSearchParseContext {

	public abstract String toSqlQuery(List<String> tempTablesCreated, String queryTable, Aggregation aggregation);
}