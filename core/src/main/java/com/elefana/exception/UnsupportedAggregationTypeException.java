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
package com.elefana.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.UNSUPPORTED_MEDIA_TYPE, reason="Unsupported aggregation type") 
public class UnsupportedAggregationTypeException extends RuntimeException {
	private static final long serialVersionUID = 4035166893370142375L;

	public UnsupportedAggregationTypeException() {
		super();
	}
	
	public UnsupportedAggregationTypeException(String aggregationName) {
		super("Unsupported aggregation type used in aggregation '" + aggregationName + "'");
	}
}