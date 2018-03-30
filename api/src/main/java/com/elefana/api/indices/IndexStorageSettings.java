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

import com.jsoniter.annotation.JsonProperty;

public class IndexStorageSettings {
	@JsonProperty("distribution")
	private DistributionMode distributionMode;
	@JsonProperty("timestamp_path")
	private String timestampPath;
	
	public DistributionMode getDistributionMode() {
		if(distributionMode == null) {
			distributionMode = DistributionMode.HASH;
		}
		return distributionMode;
	}

	public void setDistributionMode(DistributionMode distributionMode) {
		this.distributionMode = distributionMode;
	}

	public String getTimestampPath() {
		return timestampPath;
	}

	public void setTimestampPath(String timestampPath) {
		this.timestampPath = timestampPath;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionMode == null) ? 0 : distributionMode.hashCode());
		result = prime * result + ((timestampPath == null) ? 0 : timestampPath.hashCode());
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
		IndexStorageSettings other = (IndexStorageSettings) obj;
		if (distributionMode != other.distributionMode)
			return false;
		if (timestampPath == null) {
			if (other.timestampPath != null)
				return false;
		} else if (!timestampPath.equals(other.timestampPath))
			return false;
		return true;
	}
}
