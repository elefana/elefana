/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import com.jsoniter.any.Any;

public class MatchPhrasePrefixQuery extends MatchQuery {

	public MatchPhrasePrefixQuery(Any queryContext) {
		super(queryContext);
		matchMode = MatchMode.PHRASE_PREFIX;
	}

}
