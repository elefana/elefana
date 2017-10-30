/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.search.query;

import com.jsoniter.any.Any;

public class MatchPhraseQuery extends MatchQuery {

	public MatchPhraseQuery(Any queryContext) {
		super(queryContext);
		matchMode = MatchMode.PHRASE;
	}
}
