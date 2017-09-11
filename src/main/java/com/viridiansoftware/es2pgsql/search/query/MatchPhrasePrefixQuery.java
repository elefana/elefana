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
package com.viridiansoftware.es2pgsql.search.query;

import java.io.IOException;

public class MatchPhrasePrefixQuery extends MatchPhraseQuery {
	private static final String KEY_MAX_EXPANSIONS = "max_expansions";
	
	private long maxExpansions = 50;
	
	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' LIKE '" + query + "%'";
	}
	
	@Override
	public void writeNumberField(String name, int value) throws IOException {
		super.writeNumberField(name, value);
		switch (name) {
		case KEY_MAX_EXPANSIONS:
			this.maxExpansions = value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		super.writeNumberField(name, value);
		switch (name) {
		case KEY_MAX_EXPANSIONS:
			this.maxExpansions = value;
			break;
		}
	}
}
