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
package com.viridiansoftware.es2pgsql;

import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.viridiansoftware.es2pgsql.cluster.AckResponse;
import com.viridiansoftware.es2pgsql.cluster.ClusterService;
import com.viridiansoftware.es2pgsql.document.BulkService;
import com.viridiansoftware.es2pgsql.document.DocumentService;
import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.document.IndexOpType;
import com.viridiansoftware.es2pgsql.node.NodesService;
import com.viridiansoftware.es2pgsql.search.SearchService;

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
		final String indexPatternLowercase = indexPattern.toLowerCase();
		if (indexPattern == null || indexPattern.isEmpty()) {
			return clusterService.getNodeRootInfo();
		}
		switch (indexPatternLowercase) {
		case "_nodes":
			return nodesService.getNodesInfo();
		case "_mapping":
			return indexFieldMappingService.getIndexMappings();
		case "_mget":
			return documentService.multiGet();
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern:.+}", method = RequestMethod.POST)
	public Object indexOrSearch(@PathVariable String indexPattern, HttpEntity<String> request) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		switch (indexPatternLowercase) {
		case "_search":
			return searchService.search(request);
		case "_mget":
			return documentService.multiGetByRequestBody(request.getBody());
		case "_msearch":
			return searchService.multiSearch(null, null, request);
		case "_bulk":
			return bulkService.bulkOperations(request.getBody());
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern) throws Exception {
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
			switch(typePatternLowercase) {
			case "_all":
				return nodesService.getNodesInfo();
			case "_local":
				return nodesService.getLocalNodeInfo();
			default:
				return nodesService.getNodesInfo(typePattern.split(","));
			}
		default:
			break;
		}
		
		switch(typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getIndexMapping(indexPattern);
		case "_field_caps":
			return indexFieldMappingService.getFieldCapabilities(indexPattern);
		}
		return null;
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern:.+}", method = RequestMethod.POST)
	public Object indexOrSearch(@PathVariable String indexPattern, @PathVariable String typePattern,
			HttpEntity<String> request, HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		
		switch(typePatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, request);
		case "_mget":
			return documentService.multiGet(indexPattern);
		case "_field_caps":
			return indexFieldMappingService.getFieldCapabilities(indexPattern);
		case "_msearch":
			return searchService.multiSearch(indexPattern, null, request);
		}
		return indexOrSearch(indexPattern, typePattern, UUID.randomUUID().toString(), request, response);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern) throws Exception {
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
		switch(typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getIndexMapping(indexPattern, idPattern);
		}
		switch(idPatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getIndexMapping(indexPattern, typePattern);
		}
		return documentService.get(indexPattern, typePattern, idPattern);
	}

	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.POST)
	public Object indexOrSearch(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpEntity<String> request, HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		
		switch(idPatternLowercase) {
		case "_search":
			return searchService.search(indexPattern, typePattern, request);
		case "_msearch":
			return searchService.multiSearch(indexPattern, typePattern, request);
		}
		
		String document = request.getBody();
		Object result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.OVERWRITE);
		if(result != null) {
			response.setStatus(HttpServletResponse.SC_CREATED);
		}
		return result;
	}
	
	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern:.+}", method = RequestMethod.PUT)
	public Object index(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, HttpEntity<String> request, HttpServletResponse response) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		
		switch(typePatternLowercase) {
		case "_mapping":
			indexFieldMappingService.putIndexMapping(indexPattern, idPattern, request.getBody());
			return new AckResponse();
		}
		return null;
	}
	
	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpEntity<String> request) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		final String opPatternLowercase = opPattern.toLowerCase();
		
		switch(typePatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getIndexMapping(indexPattern);
		}
		
		switch(idPatternLowercase) {
		case "_mapping":
			return indexFieldMappingService.getIndexMapping(indexPattern, typePattern);
		}
		return null;
	}
	
	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern:.+}", method = RequestMethod.POST)
	public Object indexOrSearch(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, HttpEntity<String> request, HttpServletResponse response) throws Exception {
		if (idPattern.toLowerCase().equals("_search")) {
			return searchService.search(indexPattern, typePattern, request);
		}
		Object result = null;
		switch(opPattern) {
		case "_create": {
			String document = request.getBody();
			result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.CREATE);
			if(result != null) {
				response.setStatus(HttpServletResponse.SC_CREATED);
			}
			break;
		}
		case "_update": {
			String document = request.getBody();
			result = documentService.index(indexPattern, typePattern, idPattern, document, IndexOpType.UPDATE);
			if(result != null) {
				response.setStatus(HttpServletResponse.SC_ACCEPTED);
			}
			break;
		}
		}
		return result;
	}
	
	@RequestMapping(path = "/{indexPattern}/{typePattern}/{idPattern}/{opPattern}/{fieldPattern:.+}", method = RequestMethod.GET)
	public Object get(@PathVariable String indexPattern, @PathVariable String typePattern,
			@PathVariable String idPattern, @PathVariable String opPattern, @PathVariable String fieldPattern) throws Exception {
		final String indexPatternLowercase = indexPattern.toLowerCase();
		final String typePatternLowercase = typePattern.toLowerCase();
		final String idPatternLowercase = idPattern.toLowerCase();
		final String opPatternLowercase = opPattern.toLowerCase();
		final String fieldPatternLowercase = fieldPattern.toLowerCase();
		
		switch(typePatternLowercase) {
		case "_mapping":
			switch(opPatternLowercase) {
			case "field":
				return indexFieldMappingService.getIndexMapping(indexPattern, idPattern, fieldPattern);
			}
			break;
		}
		
		return null;
	}
}
