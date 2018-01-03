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

import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

public class WildcardQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";
	
	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public WildcardQuery(Any queryContext) {
		super();
		
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				value = fieldContext.get(KEY_VALUE).toString();
				
				if(!fieldContext.get(KEY_BOOST).valueType().equals(ValueType.INVALID)) {
					boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				value = fieldContext.toString();
			}
			break;
		}
	}

	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' LIKE '" + value.replace("*", "%").replace("?", "_") + "'";
	}

}
