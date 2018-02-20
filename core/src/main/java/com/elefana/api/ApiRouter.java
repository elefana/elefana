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
package com.elefana.api;

import java.net.URLDecoder;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.elefana.api.document.IndexOpType;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.cluster.ClusterService;
import com.elefana.document.BulkIngestService;
import com.elefana.document.DocumentService;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodesService;
import com.elefana.search.SearchService;

import io.netty.handler.codec.http.HttpMethod;

/**
 *
 */
@Service
public class ApiRouter {
	private static final Logger LOGGER = LoggerFactory.getLogger(ApiRouter.class);

	@Autowired
	private DocumentService documentService;
	@Autowired
	private BulkIngestService bulkIngestService;
	@Autowired
	private IndexFieldMappingService indexFieldMappingService;
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private SearchService searchService;
	@Autowired
	private NodesService nodesService;
	@Autowired
	private ClusterService clusterService;

	public ApiResponse route(HttpMethod method, String url, String requestBody) throws ElefanaException {
		if (url.length() == 1) {
			return routeToRootUrl();
		}

		final String[] urlComponents = url.startsWith("/") ? url.substring(1).split("\\/") : url.split("\\/");
		if (urlComponents[0] == null || urlComponents[0].isEmpty()) {
			return routeToRootUrl();
		}
		return routeToApi(method, url, urlComponents, requestBody);
	}

	private ApiResponse routeToRootUrl() throws ElefanaException {
		return clusterService.prepareClusterInfo().get();
	}

	private ApiResponse routeToApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			return routeToBulkApi(method, url, urlComponents, requestBody);
		case "_cluster":
			return routeToClusterApi(method, url, urlComponents, requestBody);
		case "_mapping":
			return routeToFieldMappingApi(method, url, urlComponents, requestBody);
		case "_mget":
			return routeToDocumentApi(method, url, urlComponents, requestBody);
		case "_msearch":
			return routeToSearchApi(method, url, urlComponents, requestBody);
		case "_nodes":
			return routeToNodeApi(method, url, urlComponents, requestBody);
		case "_search":
			return routeToSearchApi(method, url, urlComponents, requestBody);
		case "_template":
			return routeToIndexTemplateApi(method, url, urlComponents, requestBody);
		}

		switch (urlComponents.length) {
		case 2:
			switch (urlComponents[1].toLowerCase()) {
			case "_field_caps":
			case "_mapping":
				return routeToFieldMappingApi(method, url, urlComponents, requestBody);
			case "_msearch":
			case "_search":
				return routeToSearchApi(method, url, urlComponents, requestBody);
			}
			break;
		case 3:
			switch (urlComponents[1].toLowerCase()) {
			case "_mapping":
				return routeToFieldMappingApi(method, url, urlComponents, requestBody);
			}
			switch (urlComponents[2].toLowerCase()) {
			case "_msearch":
			case "_search":
				return routeToSearchApi(method, url, urlComponents, requestBody);
			}
			break;
		case 4:
			break;
		case 5:
			break;
		}

		if (isPutMethod(method)) {
			return routeToFieldMappingApi(method, url, urlComponents, requestBody);
		} else {
			return routeToDocumentApi(method, url, urlComponents, requestBody);
		}
	}

	private ApiResponse routeToBulkApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			return bulkIngestService.prepareBulkRequest(requestBody).get();
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToClusterApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			return clusterService.prepareClusterInfo().get();
		case 2:
			final String target = urlDecode(urlComponents[1]);
			switch (target.toLowerCase()) {
			case "health":
				return clusterService.prepareClusterHealth().get();
			case "settings":
				return clusterService.prepareClusterSettings().get();
			}
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToFieldMappingApi(HttpMethod method, String url, String[] urlComponents,
			String requestBody) throws ElefanaException {
		if (isGetMethod(method)) {
			switch (urlComponents.length) {
			case 1:
				// _mapping
				return indexFieldMappingService.prepareGetFieldMappings().get();
			case 2: {
				// INDICES/_mapping or INDICES/_field_caps
				final String indexPattern = urlDecode(urlComponents[0]);
				switch (urlComponents[1].toLowerCase()) {
				case "_mapping":
					return indexFieldMappingService.prepareGetFieldMappings(indexPattern).get();
				case "_field_caps":
					return indexFieldMappingService.prepareGetFieldCapabilities(indexPattern).get();
				}
				break;
			}
			case 3:
				// INDICES/_mapping/TYPE
				final String indexPattern = urlDecode(urlComponents[0]);
				final String typePattern = urlDecode(urlComponents[2]);
				switch (urlComponents[1].toLowerCase()) {
				case "_mapping":
					return indexFieldMappingService.prepareGetFieldMappings(indexPattern, typePattern).get();
				}
				break;
			}
		} else if (isPutMethod(method)) {
			final String index = urlDecode(urlComponents[0]);
			return indexFieldMappingService.preparePutFieldMappings(index, requestBody).get();
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToDocumentApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			switch (urlComponents[0].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(requestBody).get();
			}
			break;
		case 2: {
			// 0 = INDEX
			final String index = urlDecode(urlComponents[0]);

			switch (urlComponents[1].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(index, requestBody).get();
			}
			final String type = urlDecode(urlComponents[1]);

			if (isPostMethod(method)) {
				return documentService
						.prepareIndex(index, type, UUID.randomUUID().toString(), requestBody, IndexOpType.OVERWRITE)
						.get();
			}
			break;
		}
		case 3: {
			// 0 = INDEX, 1 = TYPE
			final String index = urlDecode(urlComponents[0]);
			final String type = urlDecode(urlComponents[1]);

			switch (urlComponents[2].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(index, type, requestBody).get();
			}
			final String id = urlDecode(urlComponents[2]);

			if (isGetMethod(method)) {
				return documentService.prepareGet(index, type, id).get();
			} else if (isPostMethod(method)) {
				return documentService.prepareIndex(index, type, id, requestBody, IndexOpType.OVERWRITE).get();
			}
			break;
		}
		case 4: {
			// 0 = INDEX, 1 = TYPE, 2 = ID
			final String index = urlDecode(urlComponents[0]);
			final String type = urlDecode(urlComponents[1]);
			final String id = urlDecode(urlComponents[2]);

			if (isPostMethod(method) || isPutMethod(method)) {
				switch (urlComponents[3].toLowerCase()) {
				case "_create": {
					return documentService.prepareIndex(index, type, id, requestBody,
							IndexOpType.CREATE).get();
				}
				case "_update": {
					return documentService.prepareIndex(index, type, id, requestBody,
							IndexOpType.UPDATE).get();
				}
				}
			}
			break;
		}
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToSearchApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			// _search
			switch (urlComponents[0].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(requestBody).get();
			case "_msearch":
				return searchService.prepareMultiSearch(requestBody).get();
			}
			break;
		case 2: {
			// INDICES/_search
			final String indexPattern = urlDecode(urlComponents[0]);
			switch (urlComponents[1].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(indexPattern, requestBody).get();
			case "_msearch":
				return searchService.prepareMultiSearch(indexPattern, requestBody).get();
			}
			break;
		}
		case 3:
			// INDICES/TYPES/_search
			final String indexPattern = urlDecode(urlComponents[0]);
			final String typePattern = urlDecode(urlComponents[1]);

			switch (urlComponents[2].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(indexPattern, typePattern, requestBody).get();
			case "_msearch":
				return searchService.prepareMultiSearch(indexPattern, typePattern, requestBody).get();
			}
			break;
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToNodeApi(HttpMethod method, String url, String[] urlComponents, String requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			return nodesService.prepareNodesInfo().get();
		case 2: {
			final String nodes = urlDecode(urlComponents[1]);
			switch (nodes.toLowerCase()) {
			case "_all":
				return nodesService.prepareNodesInfo().get();
			case "_local":
				return nodesService.prepareLocalNodeInfo().get();
			default:
				return nodesService.prepareNodesInfo(nodes.split(",")).get();
			}
		}
		case 3: {
			final String nodes = urlDecode(urlComponents[1]);
			final String[] filter = urlComponents[2].split(",");
			switch (nodes.toLowerCase()) {
			case "_all":
				return nodesService.prepareNodesInfo(filter).get();
			case "_local":
				return nodesService.prepareLocalNodeInfo(filter).get();
			default:
				return nodesService.prepareNodesInfo(nodes.split(","), filter).get();
			}
		}
		}
		throw new NoSuchApiException(url);
	}

	private ApiResponse routeToIndexTemplateApi(HttpMethod method, String url, String[] urlComponents,
			String requestBody) throws ElefanaException {
		switch (urlComponents.length) {
		case 2:
			final String templateId = urlDecode(urlComponents[1]);
			if (isGetMethod(method)) {
				return indexTemplateService.prepareGetIndexTemplate(templateId).get();
			} else if (isPostMethod(method) || isPutMethod(method)) {
				return indexTemplateService.preparePutIndexTemplate(templateId, requestBody).get();
			}
			break;
		}
		throw new NoSuchApiException(url);
	}

	private boolean isHeadMethod(HttpMethod method) {
		return method.equals(HttpMethod.HEAD);
	}

	private boolean isGetMethod(HttpMethod method) {
		return method.equals(HttpMethod.GET);
	}

	private boolean isPostMethod(HttpMethod method) {
		return method.equals(HttpMethod.POST);
	}

	private boolean isPutMethod(HttpMethod method) {
		return method.equals(HttpMethod.PUT);
	}

	private boolean isDeleteMethod(HttpMethod method) {
		return method.equals(HttpMethod.DELETE);
	}

	private String urlDecode(String url) throws ElefanaException {
		try {
			return URLDecoder.decode(url, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException();
		}
	}

	public DocumentService getDocumentApi() {
		return documentService;
	}

	public BulkIngestService getBulkApi() {
		return bulkIngestService;
	}

	public IndexFieldMappingService getFieldMappingApi() {
		return indexFieldMappingService;
	}

	public IndexTemplateService getIndexTemplateApi() {
		return indexTemplateService;
	}

	public SearchService getSearchApi() {
		return searchService;
	}

	public NodesService getNodeApi() {
		return nodesService;
	}

	public ClusterService getClusterApi() {
		return clusterService;
	}
}