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
package com.elefana.api.exception;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 *
 */
public class InvalidAggregationFieldType extends ElefanaException {
	private static final long serialVersionUID = -7458851310111019153L;

	public InvalidAggregationFieldType(String[] expectedFieldTypes, String actualFieldType) {
		super(HttpResponseStatus.PRECONDITION_FAILED, "Invalid aggregation field type - expected ["
				+ stringify(expectedFieldTypes) + "], actual '" + actualFieldType + "'");
	}

	private static String stringify(String[] expectedFieldTypes) {
		final StringBuilder result = new StringBuilder();
		for (int i = 0; i < expectedFieldTypes.length; i++) {
			if (i > 0) {
				result.append(',');
				result.append(' ');
			}
			result.append(expectedFieldTypes[i]);
		}
		return result.toString();
	}
}
