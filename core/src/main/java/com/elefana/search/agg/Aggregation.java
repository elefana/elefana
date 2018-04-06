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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.search.SearchResponse;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.PsqlQueryComponents;
import com.elefana.search.RequestBodySearch;

public abstract class Aggregation {
	public static final String AGGREGATION_TABLE_PREFIX = "elefana_agg_";
	public static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));

	public abstract void executeSqlQuery(final AggregationExec aggregationExec) throws ElefanaException;

	protected static void appendIndicesWhereClause(final AggregationExec aggregationExec,
			final StringBuilder queryBuilder) {
		if (aggregationExec.getNodeSettingsService().isUsingCitus()) {
			queryBuilder.append(" WHERE TRUE");
			return;
		}

		queryBuilder.append(" WHERE _index IN (");
		for (int i = 0; i < aggregationExec.getIndices().size(); i++) {
			if (i > 0) {
				queryBuilder.append(',');
			}
			queryBuilder.append("'");
			queryBuilder.append(aggregationExec.getIndices().get(i));
			queryBuilder.append("'");
		}
		queryBuilder.append(")");
	}

	public void executeSqlQuery(AggregationExec parentExec, PsqlQueryComponents queryComponents,
			SearchResponse searchResponse, Map<String, Object> aggregationsResult) throws ElefanaException {
		executeSqlQuery(new AggregationExec(parentExec.getIndexTemplate(), parentExec.getIndices(),
				parentExec.getTypes(), parentExec.getJdbcTemplate(), parentExec.getNodeSettingsService(),
				parentExec.getIndexFieldMappingService(), queryComponents, searchResponse, aggregationsResult,
				parentExec.getRequestBodySearch(), this));
	}

	public void executeSqlQuery(IndexTemplate indexTemplate, List<String> indices, String[] types,
			JdbcTemplate jdbcTemplate, NodeSettingsService nodeSettingsService,
			PsqlIndexFieldMappingService indexFieldMappingService, PsqlQueryComponents queryComponents,
			SearchResponse searchResponse, Map<String, Object> aggregationsResult, RequestBodySearch requestBodySearch)
			throws ElefanaException {
		executeSqlQuery(new AggregationExec(indexTemplate, indices, types, jdbcTemplate, nodeSettingsService,
				indexFieldMappingService, queryComponents, searchResponse, aggregationsResult, requestBodySearch,
				this));
	}

	public abstract String getAggregationName();

	public abstract List<Aggregation> getSubAggregations();
}
