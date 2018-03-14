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

import java.util.HashMap;
import java.util.Map;

import com.elefana.api.ApiResponse;
import com.jsoniter.annotation.JsonProperty;
import com.jsoniter.output.JsonStream;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NodesInfoResponse extends ApiResponse {
	private final Map<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	@JsonProperty("cluster_name")
	private String clusterName;
	
	public NodesInfoResponse() {
		super(HttpResponseStatus.OK.code());
	}

	public NodesInfoResponse(String clusterName) {
		super(HttpResponseStatus.OK.code());
		this.clusterName = clusterName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public Map<String, NodeInfo> getNodes() {
		return nodes;
	}

	@Override
	public String toJsonString() {
		return JsonStream.serialize(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
		result = prime * result + ((nodes == null) ? 0 : nodes.hashCode());
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
		NodesInfoResponse other = (NodesInfoResponse) obj;
		if (clusterName == null) {
			if (other.clusterName != null)
				return false;
		} else if (!clusterName.equals(other.clusterName))
			return false;
		if (nodes == null) {
			if (other.nodes != null)
				return false;
		} else if (!nodes.equals(other.nodes))
			return false;
		return true;
	}
}
