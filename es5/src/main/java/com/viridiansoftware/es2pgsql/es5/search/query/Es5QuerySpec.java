/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search.query;

import com.viridiansoftware.es2pgsql.es5.util.EsXContext;
import com.viridiansoftware.es2pgsql.search.query.QuerySpec;

public abstract class Es5QuerySpec extends EsXContext implements QuerySpec {

	public boolean isMatchAllQuery() {
		return false;
	}
}
