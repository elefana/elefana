/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.elefana.ApiRouter;
import com.elefana.document.IndexOpType;
import com.elefana.exception.ElefanaException;
import com.jsoniter.output.JsonStream;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;

/**
 *
 */
public abstract class HttpRouter extends ChannelInboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpRouter.class);
	
	private static final String CONTENT_TYPE = "Content-Type";
	private static final String APPLICATION_JSON = "application/json";

	private final ApiRouter apiRouter;
	private final Meter httpRequests;
	private final Histogram httpRequestSize;

	public HttpRouter(ApiRouter apiRouter, Meter httpRequests, Histogram httpRequestSize) {
		super();
		this.apiRouter = apiRouter;
		this.httpRequests = httpRequests;
		this.httpRequestSize = httpRequestSize;
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
	}

	public HttpResponse route(FullHttpRequest httpRequest) {
		try {
			httpRequests.mark();
			
			final String uri = httpRequest.uri();
			if(uri.length() == 1) {
				return routeToRootUrl(httpRequest);
			}
			
			final String[] urlComponents = uri.startsWith("/") ? uri.substring(1).split("\\/") : uri.split("\\/");
			if (urlComponents[0] == null || urlComponents[0].isEmpty()) {
				return routeToRootUrl(httpRequest);
			}
			return routeToApi(httpRequest, urlComponents);
		} catch (ElefanaException e) {
			LOGGER.error(e.getMessage(), e);
			return createErrorResponse(httpRequest, e);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return createResponse(httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR);
		}
	}

	private HttpResponse routeToRootUrl(FullHttpRequest httpRequest) throws ElefanaException {
		final String responseBody = JsonStream.serialize(apiRouter.getClusterApi().getNodeRootInfo());
		final FullHttpResponse result = createResponse(httpRequest, HttpResponseStatus.OK, responseBody);
		return result;
	}

	private HttpResponse routeToApi(FullHttpRequest httpRequest, String[] urlComponents) throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			return routeToBulkApi(httpRequest, urlComponents);
		case "_cluster":
			return routeToClusterApi(httpRequest, urlComponents);
		case "_mapping":
			return routeToFieldMappingApi(httpRequest, urlComponents);
		case "_mget":
			return routeToDocumentApi(httpRequest, urlComponents);
		case "_msearch":
			return routeToSearchApi(httpRequest, urlComponents);
		case "_nodes":
			return routeToNodeApi(httpRequest, urlComponents);
		case "_search":
			return routeToSearchApi(httpRequest, urlComponents);
		case "_template":
			return routeToIndexTemplateApi(httpRequest, urlComponents);
		}

		switch (urlComponents.length) {
		case 2:
			switch (urlComponents[1].toLowerCase()) {
			case "_field_caps":
			case "_mapping":
				return routeToFieldMappingApi(httpRequest, urlComponents);
			case "_msearch":
			case "_search":
				return routeToSearchApi(httpRequest, urlComponents);
			}
			break;
		case 3:
			break;
		case 4:
			break;
		case 5:
			break;
		}

		if (isPutMethod(httpRequest)) {
			return routeToFieldMappingApi(httpRequest, urlComponents);
		} else {
			return routeToDocumentApi(httpRequest, urlComponents);
		}
	}

	private HttpResponse routeToDocumentApi(FullHttpRequest httpRequest, String[] urlComponents)
			throws ElefanaException {
		final String requestBody = getRequestBody(httpRequest);

		switch (urlComponents.length) {
		case 1:
			switch (urlComponents[0].toLowerCase()) {
			case "_mget":
				return createOkResponse(httpRequest,
						JsonStream.serialize(apiRouter.getDocumentApi().multiGet(requestBody)));
			}
			break;
		case 2: {
			// 0 = INDEX
			final String index = urlComponents[0];

			switch (urlComponents[1].toLowerCase()) {
			case "_mget":
				return createOkResponse(httpRequest,
						JsonStream.serialize(apiRouter.getDocumentApi().multiGet(index, requestBody)));
			}
			final String type = urlComponents[2];

			if (isPostMethod(httpRequest)) {
				return createResponse(httpRequest, HttpResponseStatus.CREATED,
						JsonStream.serialize(apiRouter.getDocumentApi().index(index, type, UUID.randomUUID().toString(),
								getRequestBody(httpRequest), IndexOpType.OVERWRITE)));
			}
			break;
		}
		case 3: {
			// 0 = INDEX, 1 = TYPE
			final String index = urlComponents[0];
			final String type = urlComponents[1];

			switch (urlComponents[2].toLowerCase()) {
			case "_mget":
				return createOkResponse(httpRequest,
						JsonStream.serialize(apiRouter.getDocumentApi().multiGet(index, type, requestBody)));
			}
			final String id = urlComponents[2];

			if (isGetMethod(httpRequest)) {
				return createOkResponse(httpRequest, apiRouter.getDocumentApi().get(index, type, id));
			} else if (isPostMethod(httpRequest)) {
				return createResponse(httpRequest, HttpResponseStatus.CREATED, JsonStream.serialize(apiRouter
						.getDocumentApi().index(index, type, id, getRequestBody(httpRequest), IndexOpType.OVERWRITE)));
			}
			break;
		}
		case 4: {
			// 0 = INDEX, 1 = TYPE, 2 = ID
			final String index = urlComponents[0];
			final String type = urlComponents[1];
			final String id = urlComponents[2];

			if (isPostMethod(httpRequest) || isPutMethod(httpRequest)) {
				switch (urlComponents[3].toLowerCase()) {
				case "_create": {
					return createResponse(httpRequest, HttpResponseStatus.CREATED, JsonStream.serialize(apiRouter
							.getDocumentApi().index(index, type, id, getRequestBody(httpRequest), IndexOpType.CREATE)));
				}
				case "_update": {
					return createResponse(httpRequest, HttpResponseStatus.ACCEPTED, JsonStream.serialize(apiRouter
							.getDocumentApi().index(index, type, id, getRequestBody(httpRequest), IndexOpType.UPDATE)));
				}
				}
			}
			break;
		}
		}

		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToBulkApi(FullHttpRequest httpRequest, String[] urlComponents) throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			return createOkResponse(httpRequest,
					JsonStream.serialize(apiRouter.getBulkApi().bulkOperations(getRequestBody(httpRequest))));
		}

		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToFieldMappingApi(FullHttpRequest httpRequest, String[] urlComponents)
			throws ElefanaException {
		if (isGetMethod(httpRequest)) {
			switch (urlComponents.length) {
			case 1:
				//_mapping
				return createOkResponse(httpRequest, apiRouter.getFieldMappingApi().getMappings());
			case 2:
				//INDICES/_mapping or INDICES/_field_caps
				final String indexPattern = urlComponents[0];
				switch (urlComponents[1].toLowerCase()) {
				case "_mapping":
					return createOkResponse(httpRequest, apiRouter.getFieldMappingApi().getMapping(indexPattern));
				case "_field_caps":
					return createOkResponse(httpRequest, apiRouter.getFieldMappingApi().getFieldCapabilities(indexPattern));
				}
			}
		} else if (isPutMethod(httpRequest)) {
			final String index = urlComponents[0];
			apiRouter.getFieldMappingApi().putMapping(index, getRequestBody(httpRequest));
			return createOkResponse(httpRequest);
		}
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToIndexTemplateApi(FullHttpRequest httpRequest, String[] urlComponents)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 2:
			final String templateId = urlComponents[1];
			if (isGetMethod(httpRequest)) {
				return createOkResponse(httpRequest,
						JsonStream.serialize(apiRouter.getIndexTemplateApi().getIndexTemplate(templateId)));
			} else if (isPostMethod(httpRequest) || isPutMethod(httpRequest)) {
				return createOkResponse(httpRequest, JsonStream.serialize(
						apiRouter.getIndexTemplateApi().putIndexTemplate(templateId, getRequestBody(httpRequest))));
			}
		}
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToSearchApi(FullHttpRequest httpRequest, String[] urlComponents) throws ElefanaException {
		switch(urlComponents.length) {
		case 1:
			//_search
			switch(urlComponents[0].toLowerCase()) {
			case "_search":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().search(getRequestBody(httpRequest)));
			case "_msearch":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().multiSearch(getRequestBody(httpRequest)));
			}
		case 2: {
			//INDICES/_search
			final String indexPattern = urlComponents[0];
			switch(urlComponents[1].toLowerCase()) {
			case "_search":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().search(indexPattern, getRequestBody(httpRequest)));
			case "_msearch":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().multiSearch(indexPattern, getRequestBody(httpRequest)));
			}
		}
		case 3:
			//INDICES/TYPES/_search
			final String indexPattern = urlComponents[0];
			final String typePattern = urlComponents[1];
			
			switch(urlComponents[2].toLowerCase()) {
			case "_search":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().search(indexPattern, typePattern, getRequestBody(httpRequest)));
			case "_msearch":
				return createOkResponse(httpRequest, apiRouter.getSearchApi().multiSearch(indexPattern, typePattern, getRequestBody(httpRequest)));
			}
			break;
		}
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToNodeApi(FullHttpRequest httpRequest, String[] urlComponents) throws ElefanaException {
		switch(urlComponents.length) {
		case 1:
			return createOkResponse(httpRequest, apiRouter.getNodeApi().getNodesInfo());
		case 2: {
			final String nodes = urlComponents[1];
			switch(nodes.toLowerCase()) {
			case "_all":
				return createOkResponse(httpRequest, apiRouter.getNodeApi().getNodesInfo());
			case "_local":
				return createOkResponse(httpRequest, apiRouter.getNodeApi().getLocalNodeInfo());
			default:
				return createOkResponse(httpRequest,apiRouter.getNodeApi().getNodesInfo(nodes.split(",")));
			}
		}
		case 3: {
			final String nodes = urlComponents[1];
			final String [] filter = urlComponents[2].split(",");
			switch(nodes.toLowerCase()) {
			case "_all":
				return createOkResponse(httpRequest, apiRouter.getNodeApi().getNodesInfo(filter));
			case "_local":
				return createOkResponse(httpRequest, apiRouter.getNodeApi().getLocalNodeInfo(filter));
			default:
				return createOkResponse(httpRequest,apiRouter.getNodeApi().getNodesInfo(nodes.split(","), filter));
			}
		}
		}
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private HttpResponse routeToClusterApi(FullHttpRequest httpRequest, String[] urlComponents) throws ElefanaException {
		switch(urlComponents.length) {
		case 1:
			return createOkResponse(httpRequest, apiRouter.getClusterApi().getNodeRootInfo());
		case 2:
			final String target = urlComponents[1];
			switch(target.toLowerCase()) {
			case "health":
				return createOkResponse(httpRequest, apiRouter.getClusterApi().getClusterHealth());
			case "settings":
				return createOkResponse(httpRequest, apiRouter.getClusterApi().getClusterSettings());
			}
		}
		
		return createResponse(httpRequest, HttpResponseStatus.NOT_FOUND);
	}

	private FullHttpResponse createOkResponse(FullHttpRequest request) {
		return createOkResponse(request, "");
	}

	private FullHttpResponse createOkResponse(FullHttpRequest request, Map<String, Object> json) {
		return createOkResponse(request, JsonStream.serialize(json));
	}

	private FullHttpResponse createOkResponse(FullHttpRequest request, String json) {
		FullHttpResponse result = createResponse(request, HttpResponseStatus.OK);
		result.content().writeBytes(json.getBytes());
		return result;
	}

	private FullHttpResponse createErrorResponse(FullHttpRequest request, ElefanaException e) {
		FullHttpResponse result = createResponse(request, e.getStatusCode());
		result.content().writeBytes(e.getMessage().getBytes());
		result.content().writeChar('\n');
		for (int i = 0; i < e.getStackTrace().length; i++) {
			result.content().writeBytes(e.getStackTrace()[i].toString().getBytes());
			result.content().writeChar('\n');
		}
		return result;
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

	private boolean isHeadMethod(FullHttpRequest request) {
		return request.getMethod().equals(HttpMethod.HEAD);
	}

	private boolean isGetMethod(FullHttpRequest request) {
		return request.getMethod().equals(HttpMethod.GET);
	}

	private boolean isPostMethod(FullHttpRequest request) {
		return request.getMethod().equals(HttpMethod.POST);
	}

	private boolean isPutMethod(FullHttpRequest request) {
		return request.getMethod().equals(HttpMethod.PUT);
	}

	private boolean isDeleteMethod(FullHttpRequest request) {
		return request.getMethod().equals(HttpMethod.DELETE);
	}

	private String getRequestBody(FullHttpRequest request) {
		httpRequestSize.update(request.content().readableBytes());
		return request.content().toString(CharsetUtil.UTF_8);
	}
}
