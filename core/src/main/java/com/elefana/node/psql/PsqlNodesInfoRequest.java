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
package com.elefana.node.psql;

import com.elefana.api.node.NodesInfoRequest;
import com.elefana.api.node.NodesInfoResponse;

import java.util.concurrent.Callable;

public class PsqlNodesInfoRequest extends NodesInfoRequest implements Callable<NodesInfoResponse> {
	private final PsqlNodesService nodesService;
	
	public PsqlNodesInfoRequest(PsqlNodesService nodesService) {
		super(nodesService);
		this.nodesService = nodesService;
	}

	@Override
	protected Callable<NodesInfoResponse> internalExecute() {
		return this;
	}

	@Override
	public NodesInfoResponse call() throws Exception {
		return isLocalOnly() ? callLocalOnly() : callAll();
	}
	
	private NodesInfoResponse callLocalOnly() {
		if(getInfoFields() != null) {
			return nodesService.getLocalNodeInfo(getInfoFields());
		} else {
			return nodesService.getLocalNodeInfo();
		}
	}
	
	private NodesInfoResponse callAll() {
		if(getFilteredNodes() != null && getInfoFields() != null) {
			return nodesService.getNodesInfo(getFilteredNodes(), getInfoFields());
		} else if(getInfoFields() != null) {
			return nodesService.getAllNodesInfo(getInfoFields());
		} else if(getFilteredNodes() != null) {
			return nodesService.getNodesInfo(getFilteredNodes());
		} else {
			return nodesService.getAllNodesInfo();
		}
	}
}
