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

import com.elefana.api.node.NodesStatsRequest;
import com.elefana.api.node.NodesStatsResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlNodesStatsRequest extends NodesStatsRequest implements Callable<NodesStatsResponse> {
	private final PsqlNodesService nodesService;
	
	public PsqlNodesStatsRequest(PsqlNodesService nodesService, ChannelHandlerContext context) {
		super(nodesService, context);
		this.nodesService = nodesService;
	}

	@Override
	protected Callable<NodesStatsResponse> internalExecute() {
		return this;
	}

	@Override
	public NodesStatsResponse call() throws Exception {
		return isLocalOnly() ? callLocalOnly() : callAll();
	}
	
	private NodesStatsResponse callLocalOnly() {
		if(getInfoFields() != null) {
			return nodesService.getLocalNodeStats(getInfoFields());
		} else {
			return nodesService.getLocalNodeStats();
		}
	}
	
	private NodesStatsResponse callAll() {
		if(getFilteredNodes() != null && getInfoFields() != null) {
			return nodesService.getNodesStats(getFilteredNodes(), getInfoFields());
		} else if(getInfoFields() != null) {
			return nodesService.getAllNodesStats(getInfoFields());
		} else if(getFilteredNodes() != null) {
			return nodesService.getNodesStats(getFilteredNodes());
		} else {
			return nodesService.getAllNodesStats();
		}
	}
}
