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

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoSuchMappingException extends ElefanaException {
	private static final long serialVersionUID = 6356913353166607360L;

	public NoSuchMappingException(String fieldName) {
		super(HttpResponseStatus.INTERNAL_SERVER_ERROR, "No mapping for field '" + fieldName + "' was found");
	}
}
