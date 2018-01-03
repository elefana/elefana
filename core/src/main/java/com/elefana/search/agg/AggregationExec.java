/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
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
 ******************************************************************************/
package com.elefana.search.agg;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.search.RequestBodySearch;

public class AggregationExec {
	private final List<String> tableNames;
	private final String [] types;
	private final JdbcTemplate jdbcTemplate;
	private final IndexFieldMappingService indexFieldMappingService;
	private final Map<String, Object> aggregationsResult;
	private final List<String> tempTablesCreated;
	private final String queryTable;
	private final RequestBodySearch requestBodySearch;
	private final Aggregation aggregation;

	public AggregationExec(List<String> tableNames, String [] types, JdbcTemplate jdbcTemplate, IndexFieldMappingService indexFieldMappingService,
			Map<String, Object> aggregationsResult, List<String> tempTablesCreated, String queryTable,
			RequestBodySearch requestBodySearch, Aggregation aggregation) {
		super();
		this.tableNames = tableNames;
		this.types = types;
		this.jdbcTemplate = jdbcTemplate;
		this.indexFieldMappingService = indexFieldMappingService;
		this.aggregationsResult = aggregationsResult;
		this.tempTablesCreated = tempTablesCreated;
		this.queryTable = queryTable;
		this.requestBodySearch = requestBodySearch;
		this.aggregation = aggregation;
	}

	public List<String> getTableNames() {
		return tableNames;
	}

	public String[] getTypes() {
		return types;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public IndexFieldMappingService getIndexFieldMappingService() {
		return indexFieldMappingService;
	}

	public Map<String, Object> getAggregationsResult() {
		return aggregationsResult;
	}

	public List<String> getTempTablesCreated() {
		return tempTablesCreated;
	}

	public String getQueryTable() {
		return queryTable;
	}

	public RequestBodySearch getRequestBodySearch() {
		return requestBodySearch;
	}

	public Aggregation getAggregation() {
		return aggregation;
	}
}
