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

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;
import io.netty.channel.ChannelHandlerContext;

public abstract class ClusterHealthRequest extends ApiRequest<ClusterHealthResponse> {
	protected final String [] indices;

	public ClusterHealthRequest(RequestExecutor requestExecutor, ChannelHandlerContext context) {
		this(requestExecutor, context, new String [] {});
	}
	
	public ClusterHealthRequest(RequestExecutor requestExecutor, ChannelHandlerContext context, String indices) {
		this(requestExecutor, context, indices.split(","));
	}

	public ClusterHealthRequest(RequestExecutor requestExecutor, ChannelHandlerContext context, String [] indices) {
		super(requestExecutor, context);
		this.indices = indices;
	}
}
