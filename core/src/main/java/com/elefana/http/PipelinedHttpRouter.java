/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.elefana.api.ApiRouter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

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
		ctx.writeAndFlush(route((FullHttpRequest) request.getRequest()));
	}

}
