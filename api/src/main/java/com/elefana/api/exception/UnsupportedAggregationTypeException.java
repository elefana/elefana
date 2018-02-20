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

public class UnsupportedAggregationTypeException extends ElefanaException {
	private static final long serialVersionUID = 4035166893370142375L;

	public UnsupportedAggregationTypeException(String aggregationType) {
		super(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Unsupported aggregation type '" + aggregationType + "'");
	}

	public UnsupportedAggregationTypeException(String aggregationType, String aggregationName) {
		super(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
				"Unsupported aggregation type '" + aggregationType + "' used in aggregation '" + aggregationName + "'");
	}
}
