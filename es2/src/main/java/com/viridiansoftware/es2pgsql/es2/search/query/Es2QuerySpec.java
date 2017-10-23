/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search.query;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;

import com.viridiansoftware.es2pgsql.exception.UnsupportedQueryTypeException;
import com.viridiansoftware.es2pgsql.search.query.QuerySpec;

public abstract class Es2QuerySpec implements QuerySpec {

	@Override
	public boolean isMatchAllQuery() {
		return false;
	}

	public static Es2QuerySpec parseQuery(Query query) {
		if(query instanceof BooleanQuery) {
			return new BoolQuerySpec((BooleanQuery) query);
		} else if(query instanceof ConstantScoreQuery) {
			
		} else if(query instanceof MatchAllDocsQuery) {
			return new MatchAllQuerySpec();
		} else if(query instanceof MultiPhraseQuery) {
			
		} else if(query instanceof MultiPhrasePrefixQuery) {
			
		} else if(query instanceof PhraseQuery) {
			return new PhraseQuerySpec((PhraseQuery) query);
		} else if(query instanceof TermQuery) {
			return new TermQuerySpec((TermQuery) query);
		} else if(query instanceof TermsQuery) {
			
		}
		throw new UnsupportedQueryTypeException();
	}
}
