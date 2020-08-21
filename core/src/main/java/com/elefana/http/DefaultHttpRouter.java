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
import com.elefana.node.NodeSettingsService;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 *
 */
public class DefaultHttpRouter extends HttpRouter {

	public DefaultHttpRouter(ApiRouter apiRouter, NodeSettingsService nodeSettingsService,
	                         Counter httpConnections, Meter httpRequests, Histogram httpRequestSize) {
		super(apiRouter, nodeSettingsService, httpConnections, httpRequests, httpRequestSize);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (!(msg instanceof HttpRequest)) {
			return;
		}
		HttpRequest request = (HttpRequest) msg;
		HttpContent content = (HttpContent) msg;

		final boolean keepAlive = HttpUtil.isKeepAlive(request);
		try {
			write(keepAlive, ctx, route(request, content, ctx.channel().closeFuture()));
		} finally {
			ReferenceCountUtil.release(request);
		}
	}
	
	@Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
}
