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
import java.util.ArrayList;
import java.util.List;

public class IdsQuery extends QueryContext {
	private static final String KEY_TYPE = "type";
	private static final String KEY_VALUES = "values";
	private static final String KEY_BOOST = "boost";
	
	protected final List<String> types = new ArrayList<String>();
	protected final List<String> values = new ArrayList<String>();
	protected double boost = 1.0;
	
	private boolean readingTypes = false;
	private boolean readingValues = false;

	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		if(!types.isEmpty()) {
			result.append('(');
			for(int i = 0; i < types.size(); i++) {
				if(i > 0) {
					result.append(" OR ");
				}
				result.append("type = '");
				result.append(types.get(i));
				result.append("'");
			}
			result.append(')');
			if(!values.isEmpty()) {
				result.append(" AND ");
			}
		}
		result.append('(');
		for(int i = 0; i < values.size(); i++) {
			if(i > 0) {
				result.append(" OR ");
			}
			result.append("id = '");
			result.append(values.get(i));
			result.append("'");
		}
		result.append(')');
		return result.toString();
	}
	
	@Override
	public void writeFieldName(String name) throws IOException {
		switch(name) {
		case KEY_TYPE:
			readingTypes = true;
			break;
		case KEY_VALUES:
			readingValues = true;
			break;
		}
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		readingTypes = false;
		readingValues = false;
	}
	
	@Override
	public void writeString(String value) throws IOException {
		if(readingTypes) {
			types.add(value);
		} else if(readingValues) {
			values.add(value);
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
	public void writeStringField(String name, String value) throws IOException {
		switch(name) {
		case KEY_TYPE:
			types.add(value);
			break;
		}
	}
}