/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search;

import java.util.ArrayList;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.viridiansoftware.es2pgsql.search.RequestBodySearch;

public class Es5RequestBodySearch extends RequestBodySearch {
	private final SearchModule searchModule = new SearchModule(Settings.builder().build(), false, new ArrayList<SearchPlugin>());
	private final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

	public Es5RequestBodySearch(String originalQuery) throws Exception {
		this(originalQuery, false);
	}
	
	public Es5RequestBodySearch(String originalQuery, boolean debug) throws Exception {
		super(originalQuery, debug);
		queryTranslator = new Es5QueryTranslator();
		aggregationTranslator = new Es5AggregationTranslator();
		
		try {
			XContentParser xContentParser = ((Es5QueryTranslator) queryTranslator).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), originalQuery);
			QueryParseContext queryParseContext = new QueryParseContext(xContentParser);
			searchSourceBuilder.parseXContent(queryParseContext);
			
			if(debug) {
				System.out.println(searchSourceBuilder.from());
				System.out.println(searchSourceBuilder.size());
				System.out.println(searchSourceBuilder.query().toString());
				System.out.println(searchSourceBuilder.aggregations().toString());
			}
			
			from = searchSourceBuilder.from() < 0 ? 0 : searchSourceBuilder.from();
			size = searchSourceBuilder.size() < 0 ? 10 : searchSourceBuilder.size();
			
			if(searchSourceBuilder.query() != null) {
				searchSourceBuilder.query().toXContent(new XContentBuilder(((Es5QueryTranslator) queryTranslator), null), ToXContent.EMPTY_PARAMS);
			}
			if(searchSourceBuilder.aggregations() != null) {
				searchSourceBuilder.aggregations().toXContent(new XContentBuilder((Es5AggregationTranslator) aggregationTranslator, null), ToXContent.EMPTY_PARAMS);
			}
			querySqlWhereClause = queryTranslator.toSqlWhereClause();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public boolean hasAggregations() {
		if(searchSourceBuilder.aggregations() == null) {
			return false;
		}
		return searchSourceBuilder.aggregations().count() > 0;
	}

}
