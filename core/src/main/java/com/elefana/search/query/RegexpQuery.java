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

public class RegexpQuery extends Query {
	private static final String KEY_VALUE = "value";
	private static final String KEY_BOOST = "boost";

	protected String fieldName;
	protected String value;
	protected double boost = 1.0f;
	
	public RegexpQuery(Any queryContext) {
		super();
		
		for(String fieldName : queryContext.keys()) {
			this.fieldName = fieldName;
			
			Any fieldContext = queryContext.get(fieldName);
			if(fieldContext.valueType().equals(ValueType.OBJECT)) {
				this.value = fieldContext.get(KEY_VALUE).toString();
				
				if(!fieldContext.get(KEY_BOOST).equals(ValueType.INVALID)) {
					this.boost = fieldContext.get(KEY_BOOST).toDouble();
				}
			} else {
				this.value = fieldContext.toString();
			}
			break;
		}
		this.value = value.replace(".*", "%");
		this.value = value.replace(".", "_");
	}
	
	@Override
	public String toSqlWhereClause() {
		return "_source->>'" + fieldName + "' SIMILAR TO '" + value + "'";
	}
}
