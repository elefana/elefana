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
package com.elefana.api.cluster;

import com.elefana.api.ApiResponse;
import com.elefana.api.json.JsonUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

public class ClusterSettingsResponse extends ApiResponse {
	private final Map<String, Object> defaults = new HashMap<String, Object>();

	public ClusterSettingsResponse() {
		super(HttpResponseStatus.OK.code());
		
		Map<String, Object> script = new HashMap<String, Object>();
		defaults.put("script", script);
		
		Map<String, Object> engine = new HashMap<String, Object>();
		script.put("engine", engine);
		
		Map<String, Object> painless = new HashMap<String, Object>();
		painless.put("inline", false);
		engine.put("painless", painless);
		
		Map<String, Object> expression = new HashMap<String, Object>();
		expression.put("inline", false);
		engine.put("expression", expression);
		
		Map<String, Object> groovy = new HashMap<String, Object>();
		groovy.put("inline", false);
		engine.put("groovy", groovy);
		
		Map<String, Object> mustache = new HashMap<String, Object>();
		mustache.put("inline", false);
		engine.put("mustache", mustache);
	}

	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(this);
	}
}