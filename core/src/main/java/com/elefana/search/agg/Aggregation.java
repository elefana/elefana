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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.indices.IndexFieldMappingService;
import com.elefana.search.RequestBodySearch;

public abstract class Aggregation {
	public static final String AGGREGATION_TABLE_PREFIX = "es2pgsql_agg_";
	public static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));

	public abstract void executeSqlQuery(final AggregationExec aggregationExec);

	public void executeSqlQuery(AggregationExec parentExec, Map<String, Object> aggregationsResult, String queryTable) {
		executeSqlQuery(new AggregationExec(parentExec.getTableNames(), parentExec.getTypes(),
				parentExec.getJdbcTemplate(), parentExec.getIndexFieldMappingService(), aggregationsResult,
				parentExec.getTempTablesCreated(), queryTable, parentExec.getRequestBodySearch(), this));
	}

	public void executeSqlQuery(List<String> tableNames, String[] types, JdbcTemplate jdbcTemplate,
			IndexFieldMappingService indexFieldMappingService, Map<String, Object> aggregationsResult,
			List<String> tempTablesCreated, String queryTable, RequestBodySearch requestBodySearch) {
		executeSqlQuery(new AggregationExec(tableNames, types, jdbcTemplate, indexFieldMappingService, aggregationsResult,
				tempTablesCreated, queryTable, requestBodySearch, this));
	}

	public abstract String getAggregationName();

	public abstract List<Aggregation> getSubAggregations();
}
