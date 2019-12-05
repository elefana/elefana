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
package com.elefana.api.node;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public abstract class NodeStats {
	private String name;
	@JsonProperty("transport_address")
	private String transportAddress;
	private String host;
	private String ip;
	private String version;
	private String build;
	private Map<String, Object> os;
	private Map<String, Object> jvm;
	private Map<String, Object> process;
	
	public abstract boolean isMasterNode();
	
	public abstract boolean isDataNode();
	
	public abstract boolean isIngestNode();

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getTransportAddress() {
		return transportAddress;
	}
	
	public void setTransportAddress(String transportAddress) {
		this.transportAddress = transportAddress;
	}
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public String getIp() {
		return ip;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getVersion() {
		return version;
	}
	
	public void setVersion(String version) {
		this.version = version;
	}
	
	public String getBuild() {
		return build;
	}
	
	public void setBuild(String build) {
		this.build = build;
	}

	public Map<String, Object> getOs() {
		return os;
	}

	public void setOs(Map<String, Object> os) {
		this.os = os;
	}

	public Map<String, Object> getJvm() {
		return jvm;
	}

	public void setJvm(Map<String, Object> jvm) {
		this.jvm = jvm;
	}

	public Map<String, Object> getProcess() {
		return process;
	}

	public void setProcess(Map<String, Object> process) {
		this.process = process;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
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
		NodeStats other = (NodeStats) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		return true;
	}
}
