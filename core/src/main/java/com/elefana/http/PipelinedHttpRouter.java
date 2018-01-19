/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import com.elefana.ApiRouter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

/**
 *
 */
public class PipelinedHttpRouter extends HttpRouter {

	public PipelinedHttpRouter(ApiRouter apiRouter) {
		super(apiRouter);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		HttpPipelinedRequest request = (HttpPipelinedRequest) msg;
		if(!(request.getRequest() instanceof FullHttpRequest)) {
			return;
		}
		
	}

}
