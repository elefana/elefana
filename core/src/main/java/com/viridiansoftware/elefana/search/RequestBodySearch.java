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
package com.viridiansoftware.elefana.search;

import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.viridiansoftware.elefana.search.agg.AggregationsParser;
import com.viridiansoftware.elefana.search.agg.RootAggregationContext;
import com.viridiansoftware.elefana.search.query.Query;
import com.viridiansoftware.elefana.search.query.QueryParser;

public class RequestBodySearch {
	protected final String originalQuery;
	protected final long timestamp;
	
	protected final Query query;
	protected final RootAggregationContext aggregations = new RootAggregationContext();
	
	protected String querySqlWhereClause;
	protected int from;
	protected int size;

	public RequestBodySearch(String originalQuery) throws Exception {
		this(originalQuery, false);
	}
	
	public RequestBodySearch(String originalQuery, boolean debug) throws Exception {
		super();
		this.originalQuery = originalQuery;
		this.timestamp = System.currentTimeMillis();
		
		this.query = QueryParser.parseQuery(originalQuery);
		this.querySqlWhereClause = query.toSqlWhereClause();
		
		Any context = JsonIterator.deserialize(originalQuery);
		if(context.get("size").valueType().equals(ValueType.NUMBER)) {
			size = context.get("size").toInt();
		} else {
			size = 10;
		}
		if(context.get("from").valueType().equals(ValueType.NUMBER)) {
			from = context.get("from").toInt();
		} else {
			from = 0;
		}
		
		this.aggregations.setSubAggregations(AggregationsParser.parseAggregations(originalQuery));
	}
	
	public boolean hasAggregations() {
		if(aggregations.getSubAggregations() == null) {
			return false;
		}
		return !aggregations.getSubAggregations().isEmpty();
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public String getQuerySqlWhereClause() {
		return querySqlWhereClause;
	}
	
	public Query getQuery() {
		return query;
	}
	
	public RootAggregationContext getAggregations() {
		return aggregations;
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
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return Math.abs(result);
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
		if (timestamp != other.timestamp)
			return false;
		return true;
	}
}
