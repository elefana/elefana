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

import com.viridiansoftware.es2pgsql.search.QueryTranslator;

public class ConstantScoreQuery extends QueryTranslator {
	private static final String KEY_FILTER = "filter";
	private static final String KEY_BOOST = "boost";

	private double boost = 1.0;
	private boolean parsingFilter = false;

	@Override
	public void writeFieldName(String name) throws IOException {
		if (!parsingFilter) {
			switch (name) {
			case KEY_FILTER:
				parsingFilter = true;
				break;
			}
		} else {
			super.writeFieldName(name);
		}
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if(!parsingFilter) {
			switch (name) {
			case KEY_BOOST:
				this.boost = (double) value;
				break;
			}
		} else {
			super.writeNumberField(name, value);
		}
	}
	
	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if(!parsingFilter) {
			switch (name) {
			case KEY_BOOST:
				this.boost = value;
				break;
			}
		} else {
			super.writeNumberField(name, value);
		}
	}
}
