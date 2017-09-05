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

public class MatchQuery extends QuerySpec {
	private static final String KEY_QUERY = "query";
	private static final String KEY_OPERATOR = "operator";
	private static final String KEY_PREFIX_LENGTH = "prefix_length";
	private static final String KEY_MAX_EXPANSIONS = "max_expansions";
	private static final String KEY_FUZZY_TRANSPOSITIONS = "fuzzy_transpositions";
	private static final String KEY_LENIENT = "lenient";
	private static final String KEY_ZERO_TERMS_QUERY = "zero_terms_query";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String query;
	protected String operator = "OR";
	protected String zeroTermsQuery = "NONE";
	protected double boost = 1.0f;
	protected long prefixLength = 0;
	protected long maxExpansions = 50;
	protected boolean fuzzyTranspositions = true;
	protected boolean lenient = false;

	@Override
	public String toSqlWhereClause() {
		StringBuilder stringBuilder = new StringBuilder();
		String [] terms = query.split(" ");
		
		stringBuilder.append('(');
		for(int i = 0; i < terms.length; i++) {
			if(i > 0) {
				stringBuilder.append(" " + operator + " ");
			}
			stringBuilder.append("data->>'" + fieldName + "' LIKE '%" + terms[i] + "%'");
		}
		stringBuilder.append(')');
		return stringBuilder.toString();
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		switch(startObjectCount) {
		case 1:
			this.fieldName = name;
			break;
		}
	}
	
	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		switch(name) {
		case KEY_FUZZY_TRANSPOSITIONS:
			this.fuzzyTranspositions = value;
			break;
		case KEY_LENIENT:
			this.lenient = value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, double value) throws IOException {
		switch(name) {
		case KEY_BOOST:
			this.boost = value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, float value) throws IOException {
		switch(name) {
		case KEY_BOOST:
			this.boost = (double) value;
			break;
		}
	}
	
	@Override
	public void writeNumberField(String name, int value) throws IOException {
		switch (name) {
		case KEY_MAX_EXPANSIONS:
			this.maxExpansions = value;
			break;
		case KEY_PREFIX_LENGTH:
			this.prefixLength = value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		switch (name) {
		case KEY_MAX_EXPANSIONS:
			this.maxExpansions = value;
			break;
		case KEY_PREFIX_LENGTH:
			this.prefixLength = value;
			break;
		}
	}
	
	@Override
	public void writeStringField(String name, String value) throws IOException {
		switch(name) {
		case KEY_OPERATOR:
			this.operator = value;
			break;
		case KEY_QUERY:
			this.query = value;
			break;
		case KEY_ZERO_TERMS_QUERY:
			this.zeroTermsQuery = value;
			break;
		}
	}
}
