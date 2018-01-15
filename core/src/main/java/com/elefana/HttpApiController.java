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
package com.elefana;

import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.elefana.cluster.AckResponse;
import com.elefana.cluster.ClusterService;
import com.elefana.document.BulkService;
import com.elefana.document.DocumentService;
import com.elefana.document.IndexApiResponse;
import com.elefana.document.IndexOpType;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodesService;
import com.elefana.search.SearchService;

@RestController
public class HttpApiController {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpApiController.class);

	@Autowired
	private DocumentService documentService;
	@Autowired
	private BulkService bulkService;
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
	private MetricRegistry metricRegistry;

	private Meter httpRequests;
	private Histogram httpRequestSize;

	@PostConstruct
	public void postConstruct() {
		httpRequests = metricRegistry.meter(MetricRegistry.name("http", "requests"));
		httpRequestSize = metricRegistry.histogram(MetricRegistry.name("http", "requestSize"));
	}

	@RequestMapping(path = "/", method = { RequestMethod.GET, RequestMethod.HEAD })
	public Object get(HttpServletRequest request) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		return clusterService.getNodeRootInfo();
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = { RequestMethod.GET, RequestMethod.HEAD })
	public Object get(@PathVariable String indexPattern, HttpServletRequest request, HttpEntity<String> requestBody)
			throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		if (indexPattern == null || indexPattern.isEmpty()) {
			return clusterService.getNodeRootInfo();
		}
		final String indexPatternLowercase = indexPattern.toLowerCase();
		switch (indexPatternLowercase) {
		case "_nodes":
			return nodesService.getNodesInfo();
		case "_mapping":
			return indexFieldMappingService.getMappings();
		case "_mget":
			return documentService.multiGet(requestBody.getBody());
		case "_search":
			return searchService.search(requestBody);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, request, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		switch (indexPatternLowercase) {
		case "_search":
			return searchService.search(requestBody);
		case "_mget":
			return documentService.multiGet(requestBody.getBody());
		case "_msearch":
			return searchService.multiSearch(null, null, requestBody);
		case "_bulk":
			return bulkService.bulkOperations(requestBody.getBody());
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.PUT, consumes = {
			"application/x-www-form-urlencoded" })
	public Object putUrlEncoded(@PathVariable String indexPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return put(indexPattern, request, new HttpEntity<String>(param), response);
		}
		return put(indexPattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_bulk":
			return bulkService.bulkOperations(requestBody.getBody());
		}

		indexFieldMappingService.putMapping(indexPattern, requestBody.getBody());
		return new AckResponse();
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern, HttpServletRequest request,
			HttpEntity<String> requestBody) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		if (indexPattern == null || indexPattern.isEmpty()) {
			return clusterService.getNodeRootInfo();
		}
		switch (indexPatternLowercase) {
		case "_cluster":
			switch (typePatternLowercase) {
			case "health":
				return clusterService.getClusterHealth();
			case "settings":
				return clusterService.getClusterSettings();
			}
			break;
		case "_nodes":
			switch (typePatternLowercase) {
			case "_all":
				return nodesService.getNodesInfo();
			case "_local":
				return nodesService.getLocalNodeInfo();
			default:
				return nodesService.getNodesInfo(typePattern.split(","));
			}
		case "_template":
			return indexTemplateService.getIndexTemplate(typePattern);
		default:
			break;
		}

		switch (typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getMapping(indexPattern);
		case "_field_caps":
			return indexFieldMappingService.getFieldCapabilities(indexPattern);
		case "_mget":
			return documentService.multiGet(indexPattern, requestBody.getBody());
		case "_search":
			return searchService.search(indexPattern, requestBody);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			HttpServletRequest request, HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, typePattern, request, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, typePattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_template":
			indexTemplateService.putIndexTemplate(typePattern, requestBody.getBody());
			return new AckResponse();
		}

		switch (typePatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, requestBody);
		case "_mget":
			return documentService.multiGet(indexPattern, requestBody.getBody());
		case "_field_caps":
			return indexFieldMappingService.getFieldCapabilities(indexPattern);
		case "_field_stats":
			return indexFieldMappingService.getFieldStats(indexPattern);
		case "_msearch":
			return searchService.multiSearch(indexPattern, null, requestBody);
		case "_refresh":
			return null;
		}
		return post(indexPattern, typePattern, UUID.randomUUID().toString(), request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.PUT, consumes = {
			"application/x-www-form-urlencoded" })
	public Object putUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			HttpServletRequest request, HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return put(indexPattern, typePattern, request, new HttpEntity<String>(param));
		}
		return put(indexPattern, typePattern, request, requestBody);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, @PathVariable String typePattern, HttpServletRequest request,
			HttpEntity<String> requestBody) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_template":
			indexTemplateService.putIndexTemplate(typePattern, requestBody.getBody());
			return new AckResponse();
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletResponse response, HttpServletRequest request,
			HttpEntity<String> requestBody) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		switch (indexPatternLowercase) {
		case "_cluster":
			switch (typePatternLowercase) {
			case "health":
				return clusterService.getClusterHealth();
			}
			break;
		case "_nodes":
			return nodesService.getNodesInfo(typePattern.split(","));
		}
		switch (typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getMapping(indexPattern, idPattern);
		}
		switch (idPatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getMapping(indexPattern, typePattern);
		case "_mget":
			return documentService.multiGet(indexPattern, typePattern, requestBody.getBody());
		}

		Map<String, Object> result = documentService.get(indexPattern, typePattern, idPattern);
		if (!((boolean) result.get("found"))) {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		}
		return result;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, typePattern, idPattern, request, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, typePattern, idPattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();

		switch (idPatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, typePattern, requestBody);
		case "_msearch":
			return searchService.multiSearch(indexPattern, typePattern, requestBody);
		}

		String document = requestBody.getBody();
		Object result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.OVERWRITE);
		if (result != null) {
			response.setStatus(HttpServletResponse.SC_CREATED);
		}
		return result;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.PUT, consumes = {
			"application/x-www-form-urlencoded" })
	public Object putUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return put(indexPattern, typePattern, idPattern, request, new HttpEntity<String>(param), response);
		}
		return put(indexPattern, typePattern, idPattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletRequest request, HttpEntity<String> requestBody,
			HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();

		switch (typePatternLowercase) {
		case "_mapping":
			indexFieldMappingService.putMapping(indexPattern, idPattern, requestBody.getBody());
			return new AckResponse();
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpServletRequest request,
			HttpEntity<String> requestBody) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		final String opPatternLowercase = opPattern.toLowerCase();

		switch (typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getMapping(indexPattern);
		}

		switch (idPatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getMapping(indexPattern, typePattern);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, typePattern, idPattern, opPattern, request, new HttpEntity<String>(param),
					response);
		}
		return post(indexPattern, typePattern, idPattern, opPattern, request, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		if (idPattern.toLowerCase().equals("_search")) {
			return searchService.search(indexPattern, typePattern, requestBody);
		}
		IndexApiResponse result = null;
		switch (opPattern) {
		case "_create": {
			String document = requestBody.getBody();
			result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.CREATE);
			if (result != null) {
				response.setStatus(HttpServletResponse.SC_CREATED);
			}
			break;
		}
		case "_update": {
			String document = requestBody.getBody();
			result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.UPDATE);
			if (result != null) {
				result.created = false;
				response.setStatus(HttpServletResponse.SC_ACCEPTED);
			}
			break;
		}
		}
		return result;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern}/{fieldPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, @PathVariable String fieldPattern,
			HttpServletRequest request) throws Exception {
		httpRequests.mark();
		httpRequestSize.update(request.getContentLength());

		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		final String opPatternLowercase = opPattern.toLowerCase();
		final String fieldPatternLowercase = fieldPattern.toLowerCase();

		switch (typePatternLowercase) {
		case "_mapping":
			switch (opPatternLowercase) {
			case "field":
				return indexFieldMappingService.getMapping(indexPattern, idPattern, fieldPattern);
			}
			break;
		}

		return null;
	}
}
