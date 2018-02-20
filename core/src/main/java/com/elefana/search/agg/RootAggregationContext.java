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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.elefana.api.exception.ElefanaException;
import com.elefana.indices.psql.PsqlIndexFieldMappingService;
import com.elefana.node.NodeSettingsService;
import com.elefana.search.RequestBodySearch;

public class RootAggregationContext extends BucketAggregation {
	private static final Logger LOGGER = LoggerFactory.getLogger(RootAggregationContext.class);

	@Override
	public void executeSqlQuery(AggregationExec aggregationExec) throws ElefanaException {
		for (Aggregation aggregation : getSubAggregations()) {
			aggregation.executeSqlQuery(aggregationExec);
		}
	}

	@Override
	public void executeSqlQuery(AggregationExec parentExec, Map<String, Object> aggregationsResult, String queryTable) throws ElefanaException {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(parentExec, aggregationsResult, queryTable);
		}
	}

	@Override
	public void executeSqlQuery(List<String> indices, String[] types, JdbcTemplate jdbcTemplate,
			NodeSettingsService nodeSettingsService, PsqlIndexFieldMappingService indexFieldMappingService,
			Map<String, Object> aggregationsResult, List<String> tempTablesCreated, String queryTable,
			RequestBodySearch requestBodySearch) throws ElefanaException {
		for (Aggregation aggregation : subaggregations) {
			aggregation.executeSqlQuery(indices, types, jdbcTemplate, nodeSettingsService, indexFieldMappingService,
					aggregationsResult, tempTablesCreated, queryTable, requestBodySearch);
		}
	}

	@Override
	public String getAggregationName() {
		return AggregationsParser.FIELD_AGGS;
	}

	public void setSubAggregations(List<Aggregation> subAggregations) {
		if (subAggregations == null) {
			return;
		}
		this.subaggregations.clear();
		this.subaggregations.addAll(subAggregations);
	}

}
