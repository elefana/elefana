/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.viridiansoftware.es2pg.util.EsXContext;

public abstract class AggregationSpec extends EsXContext {
	protected static final String AGGREGATION_TABLE_PREFIX = "es2pgsql_agg_";

	public abstract void executeSqlQuery(final AggregationExec aggregationExec);
}