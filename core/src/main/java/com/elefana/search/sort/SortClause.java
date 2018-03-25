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
package com.elefana.search.sort;

public class SortClause {
	private final String field;
	private final boolean ascending;
	
	public SortClause(String field, boolean ascending) {
		super();
		this.field = field;
		this.ascending = ascending;
	}
	
	public String toSqlClause() {
		final StringBuilder result = new StringBuilder();
		result.append("elefana_json_field(_source, '");
		result.append(field);
		result.append("')");
		if(ascending) {
			result.append(" ASC");
		} else {
			result.append(" DESC");
		}
		return result.toString();
	}

	public String getField() {
		return field;
	}

	public boolean isAscending() {
		return ascending;
	}	
}
