/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.elefana.api.ApiRouter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

/**
 *
 */
public class DefaultHttpRouter extends HttpRouter {

	public DefaultHttpRouter(ApiRouter apiRouter, Meter httpRequests, Histogram httpRequestSize) {
		super(apiRouter, httpRequests, httpRequestSize);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		HttpRequest request = (HttpRequest) msg;
		
		if (!(request instanceof FullHttpRequest)) {
			return;
		}
		ctx.write(route((FullHttpRequest) request));
	}
	
	@Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
}
