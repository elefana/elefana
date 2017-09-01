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

public class RangeQuery extends QueryContext {
	private static final String KEY_FROM = "from";
	private static final String KEY_TO = "to";
	private static final String KEY_INCLUDE_LOWER = "include_lower";
	private static final String KEY_INCLUDE_UPPER = "include_upper";
	private static final String KEY_BOOST = "boost";

	protected String fieldName;
	protected Long longFrom, longTo;
	protected Double doubleFrom, doubleTo;
	protected boolean includeLower = false;
	protected boolean includeUpper = false;
	protected double boost = 1.0;

	protected String mostRecentFieldName = null;

	@Override
	public String toSqlWhereClause() {
		StringBuilder result = new StringBuilder();
		if (longFrom != null || doubleFrom != null) {
			result.append("data->>'" + fieldName + "' " + (includeLower ? ">=" : ">") + " "
					+ (longFrom != null ? longFrom : doubleFrom));
		}
		if (longTo != null || doubleTo != null) {
			if (longFrom != null || doubleFrom != null) {
				result.append(" AND ");
			}
			result.append("data->>'" + fieldName + "' " + (includeUpper ? "<=" : "<") + " "
					+ (longTo != null ? longTo : doubleTo));
		}
		return result.toString();
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		switch (startObjectCount) {
		case 1:
			this.fieldName = name;
			break;
		}
		mostRecentFieldName = name;
	}

	@Override
	public void writeNumber(int value) throws IOException {
		writeNumberField(mostRecentFieldName, value);
	}

	@Override
	public void writeNumber(long value) throws IOException {
		writeNumberField(mostRecentFieldName, value);
	}

	@Override
	public void writeNumber(double value) throws IOException {
		writeNumberField(mostRecentFieldName, value);
	}

	@Override
	public void writeNumber(float value) throws IOException {
		writeNumberField(mostRecentFieldName, value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		writeBooleanField(mostRecentFieldName, value);
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		if(name == null) {
			return;
		}
		switch (name) {
		case KEY_INCLUDE_LOWER:
			this.includeLower = value;
			break;
		case KEY_INCLUDE_UPPER:
			this.includeUpper = value;
			break;
		}
	}

	@Override
	public void writeNull() throws IOException {
		writeNullField(mostRecentFieldName);
	}

	@Override
	public void writeNullField(String name) throws IOException {
		if (name == null) {
			return;
		}
		switch (name) {
		case KEY_FROM:
			longFrom = null;
			doubleFrom = null;
			break;
		case KEY_TO:
			longTo = null;
			doubleTo = null;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if(name == null) {
			return;
		}
		switch (name) {
		case KEY_FROM:
			doubleFrom = value;
			break;
		case KEY_TO:
			doubleTo = value;
			break;
		case KEY_BOOST:
			boost = value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if(name == null) {
			return;
		}
		switch (name) {
		case KEY_FROM:
			doubleFrom = (double) value;
			break;
		case KEY_TO:
			doubleTo = (double) value;
			break;
		case KEY_BOOST:
			boost = (double) value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if(name == null) {
			return;
		}
		switch (name) {
		case KEY_FROM:
			longFrom = (long) value;
			break;
		case KEY_TO:
			longTo = (long) value;
			break;
		}
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if(name == null) {
			return;
		}
		switch (name) {
		case KEY_FROM:
			longFrom = value;
			break;
		case KEY_TO:
			longTo = value;
			break;
		}
	}
}
