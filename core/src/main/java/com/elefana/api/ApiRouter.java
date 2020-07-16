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

import com.elefana.api.document.IndexOpType;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.cluster.ClusterService;
import com.elefana.document.BulkIngestService;
import com.elefana.document.DocumentService;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.fieldstats.IndexFieldStatsService;
import com.elefana.node.NodesService;
import com.elefana.search.SearchService;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
	@Autowired
	private IndexFieldStatsService indexFieldStatsService;

	public ApiRequest<?> route(HttpMethod method, String url, PooledStringBuilder requestBody) throws ElefanaException {
		if (url.length() == 1) {
			return routeToRootUrl();
		}
		Map<String, String> getParams = new HashMap<>();

		if(url.contains("?")) {
			String[] urlParts = url.split("\\?");
			url = urlParts[0];

			String[] getParameterUrlPart = urlParts[1].split("&");
			for(String getParameter : getParameterUrlPart) {
				String[] getParameterKeyAndValue = getParameter.split("=");
				getParams.put(getParameterKeyAndValue[0], getParameterKeyAndValue[1]);
			}
		}
		final String[] urlComponents = url.startsWith("/") ? url.substring(1).split("\\/") : url.split("\\/");
		if (urlComponents[0] == null || urlComponents[0].isEmpty()) {
			return routeToRootUrl();
		}
		return routeToApi(method, url, getParams, urlComponents, requestBody);
	}

	private ApiRequest<?> routeToRootUrl() throws ElefanaException {
		return clusterService.prepareClusterInfo();
	}

	private ApiRequest<?> routeToApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			return routeToBulkApi(method, url, getParams, urlComponents, requestBody);
		case "_cluster":
			return routeToClusterApi(method, url, getParams, urlComponents, requestBody);
		case "_mget":
			return routeToDocumentApi(method, url, getParams, urlComponents, requestBody);
		case "_msearch":
			return routeToSearchApi(method, url, getParams, urlComponents, requestBody);
		case "_nodes":
			return routeToNodeApi(method, url, getParams, urlComponents, requestBody);
		case "_search":
			return routeToSearchApi(method, url, getParams, urlComponents, requestBody);
		case "_template":
			return routeToIndexTemplateApi(method, url, getParams, urlComponents, requestBody);
		case "_mapping":
        case "_field_stats":
            return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
		}

		switch (urlComponents.length) {
		case 2:
			switch (urlComponents[1].toLowerCase()) {
			case "_template":
				return routeToIndexTemplateApi(method, url, getParams, urlComponents, requestBody);
			case "_field_stats":
			case "_field_names":
			case "_field_caps":
			case "_mapping":
				return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
			case "_msearch":
			case "_search":
				return routeToSearchApi(method, url, getParams, urlComponents, requestBody);
			}
			break;
		case 3:
			switch (urlComponents[1].toLowerCase()) {
			case "_field_names":
			case "_mapping":
				return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
			}
			switch (urlComponents[2].toLowerCase()) {
			case "_msearch":
			case "_search":
				return routeToSearchApi(method, url, getParams, urlComponents, requestBody);
			}
			break;
		case 4:
			switch (urlComponents[1].toLowerCase()) {
			case "_mapping":
				switch (urlComponents[2].toLowerCase()) {
				case "field":
					return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
				}
			}
			break;
		case 5:
			switch (urlComponents[1].toLowerCase()) {
			case "_mapping":
				switch (urlComponents[3].toLowerCase()) {
				case "field":
					return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
				}
			}
			break;
		}

		if (isPutMethod(method)) {
			return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
		} else {
			return routeToDocumentApi(method, url, getParams, urlComponents, requestBody);
		}
	}

	private ApiRequest<?> routeToBulkApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents[0].toLowerCase()) {
		case "_bulk":
			if(urlComponents.length == 2) {
				switch(urlComponents[1].toLowerCase()) {
				case "_stats":
					return bulkIngestService.prepareBulkStatsRequest();
				default:
					return bulkIngestService.prepareBulkRequest(requestBody);
				}
			} else {
				return bulkIngestService.prepareBulkRequest(requestBody);
			}
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToClusterApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			return clusterService.prepareClusterInfo();
		case 2: {
			final String target = urlDecode(urlComponents[1]);
			switch (target.toLowerCase()) {
			case "health":
				return clusterService.prepareClusterHealth();
			case "settings":
				return clusterService.prepareClusterSettings();
			}
			break;
		}
		case 3: {
			final String target = urlDecode(urlComponents[1]);
			switch (target.toLowerCase()) {
			case "health":
				return clusterService.prepareClusterHealth(urlComponents[2]);
			}
			break;
		}
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToFieldMappingApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody) throws ElefanaException {
		if (isGetMethod(method)) {
			switch (urlComponents.length) {
			case 1:
				// _mapping
                switch (urlComponents[0].toLowerCase()) {
                    case "_mapping":
                        return indexFieldMappingService.prepareGetFieldMappings();
                    case "_field_stats":
                        String clusterLevel = getParams.getOrDefault("level", "cluster");
                        if(!getParams.containsKey("fields")) {
                            throw new NoSuchApiException(method, url + "(Get parameter fields is missing)");
                        }
                        return indexFieldStatsService.prepareGetFieldStatsGet("*", getParams.get("fields"), !clusterLevel.equals("indices"));
                }
			case 2: {
				// INDICES/_mapping | INDICES/_field_caps | INDICES/_field_names | INDICES/_field_stats
				final String indexPattern = urlDecode(urlComponents[0]);
				switch (urlComponents[1].toLowerCase()) {
				case "_field_names":
					return indexFieldStatsService.prepareGetFieldNames(indexPattern);
				case "_mapping":
					return indexFieldMappingService.prepareGetFieldMappings(indexPattern);
				case "_field_caps":
					return indexFieldMappingService.prepareGetFieldCapabilities(indexPattern);
                case "_field_stats":
                    String clusterLevel = getParams.getOrDefault("level", "cluster");
                    if(!getParams.containsKey("fields")) {
                        throw new NoSuchApiException(method, url + "(Get parameter fields is missing)");
                    }
                    return indexFieldStatsService.prepareGetFieldStatsGet(indexPattern, getParams.get("fields"), !clusterLevel.equals("indices"));
				}
				break;
			}
			case 3: {
				// INDICES/_mapping/TYPE | INDICES/_field_names/TYPE
				final String indexPattern = urlDecode(urlComponents[0]);
				final String typePattern = urlDecode(urlComponents[2]);
				switch (urlComponents[1].toLowerCase()) {
				case "_field_names":
					return indexFieldStatsService.prepareGetFieldNames(indexPattern, typePattern);
				case "_mapping":
					return indexFieldMappingService.prepareGetFieldMappings(indexPattern, typePattern);
				}
				break;
			}
			case 4: {
				// e.g. INDICES/_mapping/field/FIELD
				final String indexPattern = urlDecode(urlComponents[0]);
				final String field = urlDecode(urlComponents[3]);
				switch (urlComponents[1].toLowerCase()) {
				case "_mapping":
					switch(urlComponents[2].toLowerCase()) {
					case "field":
						return indexFieldMappingService.prepareGetFieldMappings(indexPattern, "*", field);
					}
				}
				break;
			}
			case 5: {
				// e.g. INDICES/_mapping/TYPE/field/FIELD
				final String indexPattern = urlDecode(urlComponents[0]);
				final String typePattern = urlDecode(urlComponents[2]);
				final String field = urlDecode(urlComponents[4]);
				switch (urlComponents[1].toLowerCase()) {
				case "_mapping":
					switch(urlComponents[3].toLowerCase()) {
					case "field":
						return indexFieldMappingService.prepareGetFieldMappings(indexPattern, typePattern, field);
					}
				}
				break;
			}
			}
		} else if (isPostMethod(method)) {
			switch (urlComponents.length) {
			case 1:
				switch (urlComponents[0].toLowerCase()) {
				case "_field_stats":
					String clusterLevel = getParams.getOrDefault("level", "cluster");
					return indexFieldStatsService.prepareGetFieldStatsPost("*", requestBody, !clusterLevel.equals("indices"));
				}
				break;
			case 2: {
				// INDICES/_mapping or INDICES/_field_caps
				final String indexPattern = urlDecode(urlComponents[0]);
				switch (urlComponents[1].toLowerCase()) {
				case "_field_stats":
				    String clusterLevel = getParams.getOrDefault("level", "cluster");
					return indexFieldStatsService.prepareGetFieldStatsPost(indexPattern, requestBody, !clusterLevel.equals("indices"));
				case "_refresh":
					return indexFieldMappingService.prepareRefreshIndex(indexPattern);
				}
				break;
			}
			}
		} else if (isPutMethod(method)) {
			final String index = urlDecode(urlComponents[0]);
			return indexFieldMappingService.preparePutFieldMappings(index, requestBody);
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToDocumentApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			switch (urlComponents[0].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(requestBody);
			default:
				final String index = urlDecode(urlComponents[0]);
				if(isDeleteMethod(method)) {
					return documentService.prepareDeleteIndex(index, "*");
				}
				break;
			}
			break;
		case 2: {
			// 0 = INDEX
			final String index = urlDecode(urlComponents[0]);

			switch (urlComponents[1].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(index, requestBody);
			case "_mapping":
			case "_refresh":
			case "_field_stats":
				return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
			}
			final String type = urlDecode(urlComponents[1]);

			if (isPostMethod(method)) {
				return documentService
						.prepareIndex(index, type, UUID.randomUUID().toString(), requestBody, IndexOpType.OVERWRITE);
			} else if(isDeleteMethod(method)) {
				return documentService.prepareDeleteIndex(index, type);
			}
			break;
		}
		case 3: {
			// 0 = INDEX, 1 = TYPE
			final String index = urlDecode(urlComponents[0]);
			final String type = urlDecode(urlComponents[1]);
			
			switch (urlComponents[1].toLowerCase()) {
			case "_mapping":
				return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
			}

			switch (urlComponents[2].toLowerCase()) {
			case "_mget":
				return documentService.prepareMultiGet(index, type, requestBody);
			}
			final String id = urlDecode(urlComponents[2]);

			if (isGetMethod(method) || isHeadMethod(method)) {
				return documentService.prepareGet(index, type, id, isGetMethod(method));
			} else if (isPostMethod(method)) {
				return documentService.prepareIndex(index, type, id, requestBody, IndexOpType.OVERWRITE);
			} else if (isDeleteMethod(method)) {
				return documentService.prepareDelete(index, type, id);
			}
			break;
		}
		case 4: {
			// 0 = INDEX, 1 = TYPE, 2 = ID, 3 = OP
			final String index = urlDecode(urlComponents[0]);
			final String type = urlDecode(urlComponents[1]);
			final String id = urlDecode(urlComponents[2]);

			if (isPostMethod(method) || isPutMethod(method)) {
				switch (urlComponents[3].toLowerCase()) {
				case "_create": {
					return documentService.prepareIndex(index, type, id, requestBody,
							IndexOpType.CREATE);
				}
				case "_update": {
					return documentService.prepareIndex(index, type, id, requestBody,
							IndexOpType.UPDATE);
				}
				}
			}
			break;
		}
		case 5: {
			switch (urlComponents[1].toLowerCase()) {
			case "_mapping":
				return routeToFieldMappingApi(method, url, getParams, urlComponents, requestBody);
			}
			break;
		}
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToSearchApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 1:
			// _search
			switch (urlComponents[0].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(requestBody);
			case "_msearch":
				return searchService.prepareMultiSearch(requestBody);
			}
			break;
		case 2: {
			// INDICES/_search
			final String indexPattern = urlDecode(urlComponents[0]);
			switch (urlComponents[1].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(indexPattern, requestBody);
			case "_msearch":
				return searchService.prepareMultiSearch(indexPattern, requestBody);
			}
			break;
		}
		case 3:
			// INDICES/TYPES/_search
			final String indexPattern = urlDecode(urlComponents[0]);
			final String typePattern = urlDecode(urlComponents[1]);

			switch (urlComponents[2].toLowerCase()) {
			case "_search":
				return searchService.prepareSearch(indexPattern, typePattern, requestBody);
			case "_msearch":
				return searchService.prepareMultiSearch(indexPattern, typePattern, requestBody);
			}
			break;
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToNodeApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents, PooledStringBuilder requestBody)
			throws ElefanaException {
		switch (urlComponents.length) {
		case 2:
			switch (urlComponents[1]){
				case "stats":
					// _nodes/stats
					return nodesService.prepareAllNodesStats();
			case "_local":
				// _nodes/_local
				return nodesService.prepareLocalNodeStats();
			case "_all":
				// _nodes/_all
				return nodesService.prepareAllNodesStats();
			}
			throw new NoSuchApiException(method, url);
		case 3: {
			if (urlComponents[2].equals("stats")) {
				// _nodes/node ids/stats
				final String nodes = urlDecode(urlComponents[1]);
				switch (nodes.toLowerCase()) {
					case "_all":
						return nodesService.prepareAllNodesStats();
					case "_local":
						return nodesService.prepareLocalNodeStats();
					default:
						return nodesService.prepareNodesStats(nodes.split(","));
				}
			} else if (urlComponents[1].equals("stats")) {
				// _nodes/stats/filters
				final String[] filter = urlComponents[2].split(",");
				return nodesService.prepareAllNodesStats(filter);
			}
		}
		case 4: {
			final String nodes = urlDecode(urlComponents[1]);
			final String[] filter = urlComponents[3].split(",");
			switch (urlComponents[2]) {
				case "stats":
					// _nodes/node ids/stats/filters
					switch (nodes.toLowerCase()) {
						case "_all":
							return nodesService.prepareAllNodesStats(filter);
						case "_local":
							return nodesService.prepareLocalNodeStats(filter);
						default:
							return nodesService.prepareNodesStats(nodes.split(","), filter);
					}
			}
			throw new NoSuchApiException(method, url);
		}
		}
		throw new NoSuchApiException(method, url);
	}

	private ApiRequest<?> routeToIndexTemplateApi(HttpMethod method, String url, Map<String, String> getParams, String[] urlComponents,
                                                  PooledStringBuilder requestBody) throws ElefanaException {
		switch (urlComponents.length) {
		case 2:
			switch(urlComponents[0].toLowerCase()) {
			case "_template":
				final String templateId = urlDecode(urlComponents[1]);
				if (isGetMethod(method) || isHeadMethod(method)) {
					return indexTemplateService.prepareGetIndexTemplate(templateId, isGetMethod(method));
				} else if (isPostMethod(method) || isPutMethod(method)) {
					return indexTemplateService.preparePutIndexTemplate(templateId, requestBody);
				}
				break;
			default:
				//INDEX/_template
				return indexTemplateService.prepareGetIndexTemplateForIndex(urlComponents[0]);
			}
			break;
		}
		throw new NoSuchApiException(method, url);
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
