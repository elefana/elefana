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
package com.elefana.cluster.psql;

import java.util.concurrent.Callable;

import com.elefana.api.cluster.ClusterHealthRequest;
import com.elefana.api.cluster.ClusterHealthResponse;

public class PsqlClusterHealthRequest extends ClusterHealthRequest implements Callable<ClusterHealthResponse> {
	private final PsqlClusterService clusterService;

	public PsqlClusterHealthRequest(PsqlClusterService clusterService) {
		this(clusterService, new String [] {});
	}
	
	public PsqlClusterHealthRequest(PsqlClusterService clusterService, String indices) {
		this(clusterService, indices.split(","));
	}

	public PsqlClusterHealthRequest(PsqlClusterService clusterService, String [] indices) {
		super(clusterService);
		this.clusterService = clusterService;
	}

	@Override
	protected Callable<ClusterHealthResponse> internalExecute() {
		return this;
	}

	@Override
	public ClusterHealthResponse call() throws Exception {
		return clusterService.getClusterHealth();
	}

}
