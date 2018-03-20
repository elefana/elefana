/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.elefana.api.ApiRequest;
import com.elefana.api.ApiResponse;
import com.elefana.api.ApiRouter;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.exception.NoSuchDocumentException;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

/**
 *
 */
public abstract class HttpRouter extends ChannelInboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpRouter.class);

	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON = "application/json";

	private final ApiRouter apiRouter;
	private final Counter httpConnections;
	private final Meter httpRequests;
	private final Histogram httpRequestSize;

	public HttpRouter(ApiRouter apiRouter, Counter httpConnections, Meter httpRequests, Histogram httpRequestSize) {
		super();
		this.apiRouter = apiRouter;
		this.httpConnections = httpConnections;
		this.httpRequests = httpRequests;
		this.httpRequestSize = httpRequestSize;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		httpConnections.dec();
		super.channelInactive(ctx);
	}

	public HttpResponse route(FullHttpRequest httpRequest) {
		try {
			httpRequests.mark();

			final String uri = httpRequest.uri();
			final ApiRequest<?> apiRequest = apiRouter.route(httpRequest.getMethod(), uri, getRequestBody(httpRequest));
			if(apiRequest == null) {
				throw new NoSuchApiException(uri);
			}
			final ApiResponse apiResponse = apiRequest.get();
			if(apiResponse == null) {
				throw new NoSuchApiException(uri);
			}
			return createResponse(httpRequest, HttpResponseStatus.valueOf(apiResponse.getStatusCode()), apiResponse.toJsonString());
		} catch (NoSuchDocumentException e) {
			return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND, e.getMessage());
		} catch (ElefanaException e) {
			LOGGER.error(e.getMessage(), e);
			return createErrorResponse(httpRequest, e);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return createResponse(httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
	}

	private FullHttpResponse createErrorResponse(FullHttpRequest request, ElefanaException e) {
		final StringBuilder content = new StringBuilder();
		content.append(e.getMessage());
		content.append('\n');
		for (int i = 0; i < e.getStackTrace().length; i++) {
			content.append(e.getStackTrace()[i].toString());
		}
		return createResponse(request, e.getStatusCode(), content.toString());
	}

	private FullHttpResponse createResponse(FullHttpRequest request, HttpResponseStatus status) {
		return createResponse(request, status, "");
	}

	private FullHttpResponse createResponse(FullHttpRequest request, HttpResponseStatus status, String content) {
		final FullHttpResponse result = new DefaultFullHttpResponse(request.getProtocolVersion(), status,
				Unpooled.wrappedBuffer(content.getBytes()));
		result.headers().set(CONTENT_TYPE, APPLICATION_JSON);

		if (HttpUtil.isKeepAlive(request)) {
			HttpUtil.setKeepAlive(result, true);
		}
		return result;
	}

	private String getRequestBody(FullHttpRequest request) {
		httpRequestSize.update(request.content().readableBytes());
		String charset = request.headers().contains("charset") ? request.headers().get("charset").toUpperCase() : "UTF-8";
		String result = request.content().toString(Charset.forName(charset));
//		for(Entry<String, String> header : request.headers().entries()) {
//			LOGGER.info(header.getKey() + " " + header.getValue());
//		}
		
		if(request.headers().contains("Content-Type")) {
			String contentType = request.headers().get("Content-Type").toLowerCase();
			if(contentType.contains(";")) {
				contentType = contentType.substring(0, contentType.indexOf(';'));
			}
			
			switch(contentType) {
			case "application/x-www-form-urlencoded":
				try {
					result = URLDecoder.decode(result, charset);
				} catch (UnsupportedEncodingException e) {
					LOGGER.error(e.getMessage(), e);
				}
				break;
			}
		}
		return result;
	}
}
