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
package com.elefana.api.indices;

import com.elefana.api.ApiResponse;
import com.elefana.api.json.JsonUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

public class GetFieldMappingsResponse extends ApiResponse {
	private final Map<String, Object> indicesMappings = new HashMap<String, Object>();
	
	public GetFieldMappingsResponse() {
		super(HttpResponseStatus.OK.code());
	}
	
	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(indicesMappings);
	}

	public Map<String, Object> getIndicesMappings() {
		return indicesMappings;
	}
}
