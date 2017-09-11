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
package com.viridiansoftware.es2pgsql.search;

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
	private final QueryTranslator queryTranslator = new QueryTranslator();
	
	private final int from;
	private final int size;

	public RequestBodySearch(String originalQuery) throws Exception {
		this(originalQuery, false);
	}
	
	public RequestBodySearch(String originalQuery, boolean debug) throws Exception {
		super();
		this.originalQuery = originalQuery;

		try {
			XContentParser xContentParser = queryTranslator.createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), originalQuery);
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
				searchSourceBuilder.query().toXContent(new XContentBuilder(queryTranslator, null), ToXContent.EMPTY_PARAMS);
			}
			if(searchSourceBuilder.aggregations() != null) {
				searchSourceBuilder.aggregations().toXContent(new XContentBuilder(aggregationTranslator, null), ToXContent.EMPTY_PARAMS);
			}
			querySqlWhereClause = queryTranslator.toSqlWhereClause();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public boolean hasAggregations() {
		if(searchSourceBuilder.aggregations() == null) {
			return false;
		}
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
	
	public QueryTranslator getQuery() {
		return queryTranslator;
	}
	
	public AggregationTranslator getAggregation() {
		return aggregationTranslator;
	}

	public int getFrom() {
		return from;
	}

	public int getSize() {
		return size;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((originalQuery == null) ? 0 : originalQuery.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RequestBodySearch other = (RequestBodySearch) obj;
		if (originalQuery == null) {
			if (other.originalQuery != null)
				return false;
		} else if (!originalQuery.equals(other.originalQuery))
			return false;
		return true;
	}
}
