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

import com.elefana.api.node.NodeStats;

public class V5NodeStats extends NodeStats {
	private V5HttpAttributes http;
	private V5TransportAttributes transport;
	private String [] roles;
	
	@Override
	public boolean isMasterNode() {
		if(roles == null) {
			return false;
		}
		for(String role : roles) {
			if(role.equalsIgnoreCase("master")) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isDataNode() {
		if(roles == null) {
			return false;
		}
		for(String role : roles) {
			if(role.equalsIgnoreCase("data")) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isIngestNode() {
		if(roles == null) {
			return false;
		}
		for(String role : roles) {
			if(role.equalsIgnoreCase("ingest")) {
				return true;
			}
		}
		return false;
	}

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((http == null) ? 0 : http.hashCode());
		result = prime * result + ((transport == null) ? 0 : transport.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		V5NodeStats other = (V5NodeStats) obj;
		if (http == null) {
			if (other.http != null)
				return false;
		} else if (!http.equals(other.http))
			return false;
		if (transport == null) {
			if (other.transport != null)
				return false;
		} else if (!transport.equals(other.transport))
			return false;
		return true;
	}
}
