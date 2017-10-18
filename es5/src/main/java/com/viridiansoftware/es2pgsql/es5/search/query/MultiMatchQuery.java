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
package com.viridiansoftware.es2pgsql.es5.search.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiMatchQuery extends MatchQuery {
	private static final String KEY_FIELDS = "fields";
	private static final String KEY_TYPE = "type";
	
	private List<String> fields = new ArrayList<String>();
	private List<Double> fieldBoosts = new ArrayList<Double>();
	private String type;
	
	private boolean readingFields = false;

	@Override
	public String toSqlWhereClause() {
		StringBuilder stringBuilder = new StringBuilder();
		String [] terms = query.split(" ");
		
		stringBuilder.append('(');
		for(int i = 0; i < terms.length; i++) {
			if(i > 0) {
				stringBuilder.append(" " + operator + " ");
			}
			stringBuilder.append('(');
			for(int j = 0; i < fields.size(); j++) {
				if(j > 0) {
					stringBuilder.append(" OR ");
				}
				String fieldName = fields.get(j);
				switch(type) {
				case "phrase":
					stringBuilder.append("_source->>'" + fieldName + "' LIKE '" + terms[i] + "'");
					break;
				case "phrase_prefix":
					stringBuilder.append("_source->>'" + fieldName + "' LIKE '" + terms[i] + "%'");
					break;
				default:
				case "best_fields":
					stringBuilder.append("_source->>'" + fieldName + "' LIKE '%" + terms[i] + "%'");
					break;
				}
			}
			stringBuilder.append(')');
		}
		stringBuilder.append(')');
		return stringBuilder.toString();
	}
	
	@Override
	public void writeFieldName(String name) throws IOException {
		switch(name) {
		case KEY_FIELDS:
			readingFields = true;
			break;
		}
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		if(readingFields && startArrayCount == endArrayCount) {
			readingFields = false;
		}
	}
	
	@Override
	public void writeString(String value) throws IOException {
		if(!readingFields) {
			return;
		}
		String [] components = value.split("\\^");
		fields.add(components[0]);
		fieldBoosts.add(Double.valueOf(components[1]));
	}
	
	@Override
	public void writeStringField(String name, String value) throws IOException {
		super.writeStringField(name, value);
		switch(name) {
		case KEY_TYPE:
			this.type = value;
			break;
		}
	}
}
