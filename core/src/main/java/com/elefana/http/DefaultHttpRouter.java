/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import com.elefana.ApiRouter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;

/**
 *
 */
public class DefaultHttpRouter extends HttpRouter {

	public DefaultHttpRouter(ApiRouter apiRouter) {
		super(apiRouter);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		HttpRequest request = (HttpRequest) msg;
	}

}
