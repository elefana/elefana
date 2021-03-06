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

import com.elefana.api.json.JsonUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

public class NoSuchDocumentException extends ElefanaException {
	private static final long serialVersionUID = 923131659302915891L;
	
	private final String index, type, id;

	public NoSuchDocumentException(String index, String type, String id) {
		super(HttpResponseStatus.NOT_FOUND, "Document not found - Index: " + index + ", Type: " + type + ", Id: " + id);
		this.index = index;
		this.type = type;
		this.id = id;
	}
	
	@Override
	public String getMessage() {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("_index", index);
		result.put("_type", type);
		result.put("_id", id);
		result.put("found", false);
		return JsonUtils.toJsonString(result);
	}
}
