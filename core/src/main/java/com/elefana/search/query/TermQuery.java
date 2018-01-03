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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class TermQuery extends Query {
	private static final Logger LOGGER = LoggerFactory.getLogger(TermQuery.class);
	
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public TermQuery(Any queryContext) {
		super();
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				this.value = fieldContext.get(KEY_VALUE).toString();
				if(fieldContext.keys().contains(KEY_BOOST)) {
					this.boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				this.value = fieldContext.toString();
			}
		}
	}

	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' = '" + value + "'";
	}

}
