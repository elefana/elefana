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
package com.elefana.api.node.v2;

import com.elefana.api.node.NodeInfo;
import com.jsoniter.annotation.JsonProperty;

public class V2NodeInfo extends NodeInfo {
	private final V2NodeAttributes attributes = new V2NodeAttributes();
	@JsonProperty("http_address")
	private String httpAddress;

	public String getHttpAddress() {
		return httpAddress;
	}

	public void setHttpAddress(String httpAddress) {
		this.httpAddress = httpAddress;
	}

	public V2NodeAttributes getAttributes() {
		return attributes;
	}
}
