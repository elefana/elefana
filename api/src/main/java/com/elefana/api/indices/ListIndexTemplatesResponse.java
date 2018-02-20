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

import java.util.HashMap;
import java.util.Map;

import com.elefana.api.ApiResponse;
import com.jsoniter.output.JsonStream;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ListIndexTemplatesResponse extends ApiResponse {
	private final Map<String, IndexTemplate> templates = new HashMap<String, IndexTemplate>(1);
	
	public ListIndexTemplatesResponse() {
		super(HttpResponseStatus.OK.code());
	}

	public  Map<String, IndexTemplate> getTemplates() {
		return templates;
	}
	
	@Override
	public String toJsonString() {
		return JsonStream.serialize(this);
	}
}