/**
 * Copyright 2017 Viridian Software Ltd.
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
 */
package com.viridiansoftware.elefana;

import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.viridiansoftware.elefana.cluster.AckResponse;
import com.viridiansoftware.elefana.cluster.ClusterService;
import com.viridiansoftware.elefana.document.BulkService;
import com.viridiansoftware.elefana.document.DocumentService;
import com.viridiansoftware.elefana.document.IndexApiResponse;
import com.viridiansoftware.elefana.document.IndexOpType;
import com.viridiansoftware.elefana.indices.IndexFieldMappingService;
import com.viridiansoftware.elefana.indices.IndexTemplateService;
import com.viridiansoftware.elefana.node.NodesService;
import com.viridiansoftware.elefana.search.SearchService;

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

	@RequestMapping(path = "/", method = { RequestMethod.GET, RequestMethod.HEAD })
	public Object get() throws Exception {
		return clusterService.getNodeRootInfo();
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = { RequestMethod.GET, RequestMethod.HEAD })
	public Object get(@PathVariable String indexPattern, HttpEntity<String> request) throws Exception {
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
			return documentService.multiGet(request.getBody());
		case "_search":
			return searchService.search(request);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, HttpEntity<String> request, HttpServletResponse response)
			throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		switch (indexPatternLowercase) {
		case "_search":
			return searchService.search(request);
		case "_mget":
			return documentService.multiGet(request.getBody());
		case "_msearch":
			return searchService.multiSearch(null, null, request);
		case "_bulk":
			return bulkService.bulkOperations(request.getBody());
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.PUT, consumes = {
			"application/x-www-form-urlencoded" })
	public Object putUrlEncoded(@PathVariable String indexPattern, HttpServletRequest request,
			HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return put(indexPattern, new HttpEntity<String>(param), response);
		}
		return put(indexPattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, HttpEntity<String> request, HttpServletResponse response)
			throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_bulk":
			return bulkService.bulkOperations(request.getBody());
		}

		indexFieldMappingService.putMapping(indexPattern, request.getBody());
		return new AckResponse();
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern, HttpEntity<String> request)
			throws Exception {
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
			return documentService.multiGet(indexPattern, request.getBody());
		case "_search":
			return searchService.search(indexPattern, request);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.POST, consumes = {
			"application/x-www-form-urlencoded" })
	public Object postUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			HttpServletRequest request, HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return post(indexPattern, typePattern, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, typePattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern, HttpEntity<String> request,
			HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_template":
			indexTemplateService.putIndexTemplate(typePattern, request.getBody());
			return new AckResponse();
		}

		switch (typePatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, request);
		case "_mget":
			return documentService.multiGet(indexPattern, request.getBody());
		case "_field_caps":
			return indexFieldMappingService.getFieldCapabilities(indexPattern);
		case "_field_stats":
			return indexFieldMappingService.getFieldStats(indexPattern);
		case "_msearch":
			return searchService.multiSearch(indexPattern, null, request);
		case "_refresh":
			return null;
		}
		return post(indexPattern, typePattern, UUID.randomUUID().toString(), request, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.PUT, consumes = {
			"application/x-www-form-urlencoded" })
	public Object putUrlEncoded(@PathVariable String indexPattern, @PathVariable String typePattern,
			HttpServletRequest request, HttpEntity<String> requestBody, HttpServletResponse response) throws Exception {
		for (String param : request.getParameterMap().keySet()) {
			return put(indexPattern, typePattern, new HttpEntity<String>(param));
		}
		return put(indexPattern, typePattern, requestBody);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, @PathVariable String typePattern, HttpEntity<String> request)
			throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();

		switch (indexPatternLowercase) {
		case "_template":
			indexTemplateService.putIndexTemplate(typePattern, request.getBody());
			return new AckResponse();
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpServletResponse response, HttpEntity<String> request) throws Exception {
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
			return documentService.multiGet(indexPattern, typePattern, request.getBody());
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
			return post(indexPattern, typePattern, idPattern, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, typePattern, idPattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpEntity<String> request, HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();

		switch (idPatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, typePattern, request);
		case "_msearch":
			return searchService.multiSearch(indexPattern, typePattern, request);
		}

		String document = request.getBody();
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
			return put(indexPattern, typePattern, idPattern, new HttpEntity<String>(param), response);
		}
		return put(indexPattern, typePattern, idPattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.PUT, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object put(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpEntity<String> request, HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();

		switch (typePatternLowercase) {
		case "_mapping":
			indexFieldMappingService.putMapping(indexPattern, idPattern, request.getBody());
			return new AckResponse();
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpEntity<String> request)
			throws Exception {
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
			return post(indexPattern, typePattern, idPattern, opPattern, new HttpEntity<String>(param), response);
		}
		return post(indexPattern, typePattern, idPattern, opPattern, requestBody, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.POST, consumes = {
			"!application/x-www-form-urlencoded" })
	public Object post(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpEntity<String> request,
			HttpServletResponse response) throws Exception {
		if (idPattern.toLowerCase().equals("_search")) {
			return searchService.search(indexPattern, typePattern, request);
		}
		IndexApiResponse result = null;
		switch (opPattern) {
		case "_create": {
			String document = request.getBody();
			result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.CREATE);
			if (result != null) {
				response.setStatus(HttpServletResponse.SC_CREATED);
			}
			break;
		}
		case "_update": {
			String document = request.getBody();
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
			@PathVariable String idPattern, @PathVariable String opPattern, @PathVariable String fieldPattern)
			throws Exception {
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
