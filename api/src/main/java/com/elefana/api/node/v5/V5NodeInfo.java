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
package com.elefana.api.node.v5;

import com.elefana.api.node.NodeInfo;

public class V5NodeInfo extends NodeInfo {
	private V5HttpAttributes http;
	private V5TransportAttributes transport;
	private String [] roles;

	public String[] getRoles() {
		return roles;
	}

	public void setRoles(String[] roles) {
		this.roles = roles;
	}

	public V5HttpAttributes getHttp() {
		return http;
	}

	public void setHttp(V5HttpAttributes http) {
		this.http = http;
	}

	public V5TransportAttributes getTransport() {
		return transport;
	}

	public void setTransport(V5TransportAttributes transport) {
		this.transport = transport;
	}
}
