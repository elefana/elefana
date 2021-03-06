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
import com.elefana.api.ApiRequest;
import com.elefana.api.ApiResponse;
import com.elefana.api.ApiRouter;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.exception.NoSuchDocumentException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.node.NodeSettingsService;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;

/**
 *
 */
public abstract class HttpRouter extends ChannelInboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpRouter.class);

	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String HEADER_VALUE_APPLICATION_JSON = "application/json";
	private static final String HEADER_CHARSET = "charset";
	private static final String HEADER_VALUE_UTF8 = "utf-8";
	private static final Charset CHARSET = Charset.forName("UTF-8");

	private final ApiRouter apiRouter;
	private final NodeSettingsService nodeSettingsService;
	private final Counter httpConnections;
	private final Meter httpRequests;
	private final Histogram httpRequestSize;

	private final long httpTimeoutMillis;

	public HttpRouter(ApiRouter apiRouter, NodeSettingsService nodeSettingsService,
	                  Counter httpConnections, Meter httpRequests, Histogram httpRequestSize) {
		super();
		this.apiRouter = apiRouter;
		this.nodeSettingsService = nodeSettingsService;
		this.httpConnections = httpConnections;
		this.httpRequests = httpRequests;
		this.httpRequestSize = httpRequestSize;

		this.httpTimeoutMillis = TimeUnit.SECONDS.toMillis(nodeSettingsService.getHttpTimeout());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if(httpConnections != null) {
			httpConnections.dec();
		}
		super.channelInactive(ctx);
	}
	
	public void write(long startTime, boolean keepAlive, ChannelHandlerContext ctx, HttpResponse response) {
		if(!ctx.channel().isWritable()) {
			if(System.currentTimeMillis() - startTime >= httpTimeoutMillis) {
				ctx.close();
				return;
			}
			ctx.flush();
			ctx.executor().submit(() -> {
				write(startTime, keepAlive, ctx, response);
			});
			return;
		}
		if(keepAlive && !isErrorResponse(response)) {
			if (response instanceof FullHttpMessage) {
				FullHttpMessage fullHttpMessage = (FullHttpMessage) response;
				response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
						fullHttpMessage.content().readableBytes());
				response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			}
		} else {
			response.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
		}

		ChannelFuture writeFuture = ctx.write(response);

		if (!keepAlive) {
			ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		} else if(isErrorResponse(response)) {
			ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		} else {
			ctx.flush();
		}
	}

	public void route(final boolean keepAlive, final ChannelHandlerContext context,
					  HttpRequest httpRequest, HttpContent httpContent, ChannelFuture closeFuture) {
		final String uri = httpRequest.uri();
		final PooledStringBuilder requestContent = getRequestBody(httpRequest, httpContent);
		try {
			httpRequests.mark();

			if(apiRouter == null) {
				requestContent.release();
				write(System.currentTimeMillis(), keepAlive, context, createResponse(httpRequest, HttpResponseStatus.OK));
				return;
			}

			final ApiRequest<?> apiRequest = apiRouter.route(context, httpRequest.getMethod(), uri, requestContent);
			if(apiRequest == null) {
				requestContent.release();
				write(System.currentTimeMillis(), keepAlive, context, createResponse(httpRequest, HttpResponseStatus.NOT_FOUND));
				return;
			}
			final GenericFutureListener closeListener = new GenericFutureListener<Future<? super Void>>() {
				@Override
				public void operationComplete(Future<? super Void> future) throws Exception {
					try {
						apiRequest.cancel();
					} catch (Exception e) {
						LOGGER.error("[" + uri + "] " + e.getMessage(), e);
					}
				}
			};
			closeFuture.addListener(closeListener);
			apiRequest.addListener(new GenericFutureListener<Future<? super Void>>() {
				@Override
				public void operationComplete(Future<? super Void> future) throws Exception {
					closeFuture.removeListener(closeListener);

					try {
						final HttpResponse httpResponse;
						final ApiResponse apiResponse = apiRequest.get();
						requestContent.release();

						if (apiResponse == null) {
							httpResponse = createResponse(httpRequest, HttpResponseStatus.NOT_FOUND, future.cause().getMessage());
						} else if (future.isSuccess()) {
							httpResponse = createResponse(httpRequest, HttpResponseStatus.valueOf(apiResponse.getStatusCode()), apiResponse.toJsonString());
						} else {
							if (future.cause() instanceof NoSuchDocumentException) {
								httpResponse = createResponse(httpRequest, HttpResponseStatus.NOT_FOUND, future.cause().getMessage());
							} else if (future.cause() instanceof ElefanaException) {
								LOGGER.error(uri);
								LOGGER.error(requestContent.toString());
								LOGGER.error(future.cause().getMessage(), future.cause());
								httpResponse = createErrorResponse(httpRequest, (ElefanaException) future.cause());
							} else {
								if (future.cause().getCause() instanceof ElefanaException) {
									httpResponse = createErrorResponse(httpRequest, (ElefanaException) future.cause().getCause());
								} else {
									httpResponse = createResponse(httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR);
								}
							}
						}

						context.executor().submit(() -> {
							write(System.currentTimeMillis(), keepAlive, context, httpResponse);
						});
					} catch (ShardFailedException e) {
						LOGGER.error("[" + uri + "] " + e.getMessage(), e);
						context.executor().submit(() -> {
							write(System.currentTimeMillis(), keepAlive, context, createResponse(httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
						});
					} catch (Exception e) {
						LOGGER.error("[" + uri + "] " + e.getMessage(), e);
						context.close();
					}
				}
			});
			apiRequest.execute();
			return;
		} catch (Exception e) {
			LOGGER.error(requestContent.toString());
			LOGGER.error("[" + uri + "] " + e.getMessage(), e);
			write(System.currentTimeMillis(), keepAlive, context, createResponse(httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
			return;
		}
	}

	private FullHttpResponse createErrorResponse(HttpRequest request, ElefanaException e) {
		if(e.getStatusCode().equals(HttpResponseStatus.INTERNAL_SERVER_ERROR)) {
			LOGGER.error(e.getMessage(), e);
		}

		final StringBuilder content = new StringBuilder();
		content.append(e.getMessage());
		content.append('\n');
		for (int i = 0; i < e.getStackTrace().length; i++) {
			content.append(e.getStackTrace()[i].toString());
		}
		return createResponse(request, e.getStatusCode(), content.toString());
	}

	private FullHttpResponse createResponse(HttpRequest request, HttpResponseStatus status) {
		return createResponse(request, status, "");
	}

	private FullHttpResponse createResponse(HttpRequest request, HttpResponseStatus status, String content) {
		final FullHttpResponse result = new DefaultFullHttpResponse(request.getProtocolVersion(), status,
				Unpooled.wrappedBuffer((content == null ? "" : content).getBytes(CHARSET)));
		result.headers().set(HEADER_CHARSET, HEADER_VALUE_UTF8);
		result.headers().set(HEADER_CONTENT_TYPE, HEADER_VALUE_APPLICATION_JSON);

		if (HttpUtil.isKeepAlive(request)) {
			HttpUtil.setKeepAlive(result, true);
		}
		return result;
	}

	private PooledStringBuilder getRequestBody(HttpRequest request, HttpContent content) {
		if(httpRequestSize != null) {
			httpRequestSize.update(content.content().readableBytes());
		}
		String charset = request.headers().contains("charset") ? request.headers().get("charset").toUpperCase() : "UTF-8";
		PooledStringBuilder result = PooledStringBuilder.allocate();

		if(request.headers().contains("Content-Type")) {
			String contentType = request.headers().get("Content-Type").toLowerCase();
			if(contentType.contains(";")) {
				contentType = contentType.substring(0, contentType.indexOf(';'));
			}
			
			switch(contentType) {
			case "application/x-www-form-urlencoded":
				result.appendUrlDecode(content.content(), charset);
				break;
			default:
				result.append(content.content(), Charset.forName(charset));
				break;
			}
		} else {
			result.append(content.content(), Charset.forName(charset));
		}
		return result;
	}

	private boolean isErrorResponse(HttpResponse response) {
		if(response.status().code() >= 400 && response.status().code() < 600) {
			return true;
		}
		return false;
	}
}
