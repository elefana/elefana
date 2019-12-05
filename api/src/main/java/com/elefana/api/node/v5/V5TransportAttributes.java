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

import com.elefana.api.json.EmptyJsonObject;
import com.fasterxml.jackson.annotation.JsonProperty;

public class V5TransportAttributes {
	@JsonProperty("bound_address")
	private String [] boundAddress;
	@JsonProperty("publish_address")
	private String publishAddress;
	private EmptyJsonObject profiles = EmptyJsonObject.INSTANCE;

	public String [] getBoundAddress() {
		return boundAddress;
	}

	public void setBoundAddress(String [] boundAddress) {
		this.boundAddress = boundAddress;
	}

	public String getPublishAddress() {
		return publishAddress;
	}

	public void setPublishAddress(String publishAddress) {
		this.publishAddress = publishAddress;
	}

	public EmptyJsonObject getProfiles() {
		return profiles;
	}

	public void setProfiles(EmptyJsonObject profiles) {
		this.profiles = profiles;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((publishAddress == null) ? 0 : publishAddress.hashCode());
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
		V5TransportAttributes other = (V5TransportAttributes) obj;
		if (publishAddress == null) {
			if (other.publishAddress != null)
				return false;
		} else if (!publishAddress.equals(other.publishAddress))
			return false;
		return true;
	}
}
