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
package com.elefana.api.document;

import com.elefana.api.ApiResponse;
import com.elefana.api.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

public class GetResponse extends ApiResponse {
	@JsonProperty("_index")
	private String index;
	@JsonProperty("_type")
	private String type;
	@JsonProperty("_id")
	private String id;
	@JsonProperty("_version")
	private int version = 1;
	@JsonProperty("found")
	private boolean found = true;
	@JsonProperty("_source")
	private Map source;
	
	public GetResponse() {
		super(HttpResponseStatus.OK.code());
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Map getSource() {
		return source;
	}

	public void setSource(Map source) {
		this.source = source;
	}

	public boolean isFound() {
		return found;
	}

	public void setFound(boolean found) {
		this.found = found;
		if(found) {
			setStatusCode(HttpResponseStatus.OK.code());
		} else {
			setStatusCode(HttpResponseStatus.NOT_FOUND.code());
		}
	}
	
	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(this);
	}
}
