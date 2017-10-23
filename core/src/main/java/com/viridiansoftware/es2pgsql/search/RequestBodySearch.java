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

import com.viridiansoftware.es2pgsql.search.query.QuerySpec;

public abstract class RequestBodySearch {
	protected final String originalQuery;
	
	protected AggregationTranslator aggregationTranslator;
	protected QuerySpec queryTranslator;
	
	protected String querySqlWhereClause;
	protected int from;
	protected int size;

	public RequestBodySearch(String originalQuery) throws Exception {
		this(originalQuery, false);
	}
	
	public RequestBodySearch(String originalQuery, boolean debug) throws Exception {
		super();
		this.originalQuery = originalQuery;
	}
	
	public abstract boolean hasAggregations();

	public String getOriginalQuery() {
		return originalQuery;
	}

	public String getQuerySqlWhereClause() {
		return querySqlWhereClause;
	}
	
	public QuerySpec getQuery() {
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
