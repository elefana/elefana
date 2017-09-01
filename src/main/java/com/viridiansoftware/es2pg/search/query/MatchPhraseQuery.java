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
package com.viridiansoftware.es2pg.search.query;

import java.io.IOException;

public class MatchPhraseQuery extends QueryContext {
	private static final String KEY_QUERY = "query";
	private static final String KEY_SLOP = "slop";
	private static final String KEY_BOOST = "boost";

	protected String fieldName;
	protected String query;
	protected long slop = 0;
	protected double boost = 1.0;

	@Override
	public String toSqlWhereClause() {
		return "data->>'" + fieldName + "' LIKE '" + query + "'";
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		switch (startObjectCount) {
		case 1:
			this.fieldName = name;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		switch (name) {
		case KEY_BOOST:
			this.boost = value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		switch (name) {
		case KEY_BOOST:
			this.boost = (double) value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		switch (name) {
		case KEY_SLOP:
			this.slop = value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		switch (name) {
		case KEY_SLOP:
			this.slop = value;
			break;
		}
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		switch (name) {
		case KEY_QUERY:
			this.query = value;
			break;
		}
	}

}
