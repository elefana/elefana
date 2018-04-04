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
import com.jsoniter.any.Any;

public class ExistsQuery extends Query {
	private static final String KEY_FIELD = "field";
	
	private String fieldName;
	
	public ExistsQuery(Any queryContext) {
		super();
		this.fieldName = queryContext.get(KEY_FIELD).toString();
	}
	
	@Override
	public String toSqlWhereClause(IndexTemplate indexTemplate) {
		return "_source ? '" + fieldName + "'";
	}
}
