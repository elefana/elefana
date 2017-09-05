/**
 * Copyright 2017 Viridian Software Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.viridiansoftware.es2pg.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

public class RequestBodySearch {
	private final SearchModule searchModule = new SearchModule(Settings.builder().build(), false, new ArrayList<SearchPlugin>());
	private final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	private final String originalQuery;
	private final String querySqlWhereClause;
	private final AggregationTranslator aggregationTranslator = new AggregationTranslator();

	public RequestBodySearch(String originalQuery) throws Exception {
		this(originalQuery, false);
	}
	
	public RequestBodySearch(String originalQuery, boolean debug) throws Exception {
		super();
		this.originalQuery = originalQuery;

		try {
			final QueryTranslator queryTranslator = new QueryTranslator();
			XContentParser xContentParser = queryTranslator.createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), originalQuery);
			QueryParseContext queryParseContext = new QueryParseContext(xContentParser);
			searchSourceBuilder.parseXContent(queryParseContext);
			
			if(debug) {
				System.out.println(searchSourceBuilder.query().toString());
				System.out.println(searchSourceBuilder.aggregations().toString());
			}
			
			searchSourceBuilder.query().toXContent(new XContentBuilder(queryTranslator, null), ToXContent.EMPTY_PARAMS);
			searchSourceBuilder.aggregations().toXContent(new XContentBuilder(aggregationTranslator, null), ToXContent.EMPTY_PARAMS);
			querySqlWhereClause = queryTranslator.toSqlWhereClause();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public boolean hasAggregations() {
		return searchSourceBuilder.aggregations().count() > 0;
	}

	public SearchSourceBuilder getSearchSourceBuilder() {
		return searchSourceBuilder;
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public String getQuerySqlWhereClause() {
		return querySqlWhereClause;
	}
	
	public AggregationTranslator getAggregation() {
		return aggregationTranslator;
	}
}
