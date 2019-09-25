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

import com.elefana.api.node.NodeStats;
import com.jsoniter.annotation.JsonProperty;

public class V2NodeStats extends NodeStats {
	private final V2NodeAttributes attributes = new V2NodeAttributes();
	@JsonProperty("http_address")
	private String httpAddress;
	
	@Override
	public boolean isMasterNode() {
		return attributes.isMaster();
	}

	@Override
	public boolean isDataNode() {
		return attributes.isData();
	}

	@Override
	public boolean isIngestNode() {
		return attributes.isIngest();
	}
	
	public V2NodeStats() {
		super();
	}
	
	public V2NodeStats(String httpAddress) {
		super();
		this.httpAddress = httpAddress;
	}

	public String getHttpAddress() {
		return httpAddress;
	}

	public void setHttpAddress(String httpAddress) {
		this.httpAddress = httpAddress;
	}

	public V2NodeAttributes getAttributes() {
		return attributes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((httpAddress == null) ? 0 : httpAddress.hashCode());
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
		V2NodeStats other = (V2NodeStats) obj;
		if (httpAddress == null) {
			if (other.httpAddress != null)
				return false;
		} else if (!httpAddress.equals(other.httpAddress))
			return false;
		return true;
	}
}
