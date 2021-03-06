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
package com.elefana.search.query;

import com.elefana.api.indices.IndexTemplate;
import com.elefana.indices.fieldstats.IndexFieldStatsService;

import java.util.List;

public class MatchAllQuery extends Query {
	
	@Override
	public boolean isMatchAllQuery() {
		return true;
	}

	@Override
	public String toSqlWhereClause(List<String> indices,  IndexTemplate indexTemplate,
	                               IndexFieldStatsService indexFieldStatsService) {
		return "";
	}

}
