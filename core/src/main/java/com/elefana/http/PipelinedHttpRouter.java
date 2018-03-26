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
package com.elefana.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.elefana.api.ApiRouter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;

/**
 *
 */
public class PipelinedHttpRouter extends HttpRouter {

	public PipelinedHttpRouter(ApiRouter apiRouter, Counter httpConnections, Meter httpRequests, Histogram httpRequestSize) {
		super(apiRouter, httpConnections, httpRequests, httpRequestSize);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		HttpPipelinedRequest request = (HttpPipelinedRequest) msg;
				
		if(!(request.getRequest() instanceof FullHttpRequest)) {
			return;
		}
		try {
			HttpResponse httpResponse = route((FullHttpRequest) request.getRequest());
			final ChannelPromise channelPromise = ctx.channel().newPromise();
			ctx.write(request.createHttpResponse(httpResponse, channelPromise), channelPromise);
		} finally {
			ReferenceCountUtil.release(request.getRequest());
		}
	}

}
