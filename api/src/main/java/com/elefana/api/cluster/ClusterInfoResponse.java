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

public class ClusterInfoResponse extends ApiResponse {
	private final ClusterVersionInfo version = new ClusterVersionInfo();
	private String name;
	private String clusterName;
	private String clusterUuid;
	private String tagline;

	public ClusterInfoResponse() {
		super(HttpResponseStatus.OK.code());
	}

	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(this);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getClusterUuid() {
		return clusterUuid;
	}

	public void setClusterUuid(String clusterUuid) {
		this.clusterUuid = clusterUuid;
	}

	public String getTagline() {
		return tagline;
	}

	public void setTagline(String tagline) {
		this.tagline = tagline;
	}

	public ClusterVersionInfo getVersion() {
		return version;
	}

}
