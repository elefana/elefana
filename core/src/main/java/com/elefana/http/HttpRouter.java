/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import com.elefana.ApiRouter;

import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

/**
 *
 */
public abstract class HttpRouter extends SimpleChannelInboundHandler<Object> {
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON = "application/json";
	
	private final ApiRouter apiRouter;

	public HttpRouter(ApiRouter apiRouter) {
		super();
		this.apiRouter = apiRouter;
	}
	
	public HttpResponse route(HttpRequest httpRequest) {
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}
	
	private HttpResponse createResponse(HttpRequest request, HttpResponseStatus status) {
		final FullHttpResponse result = new DefaultFullHttpResponse(request.getProtocolVersion(), status);
		result.headers().set(CONTENT_TYPE, APPLICATION_JSON);
		
		if(HttpUtil.isKeepAlive(request)) {
			HttpUtil.setKeepAlive(result, true);
		}
		return result;
	}
}
