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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.codahale.metrics.MetricRegistry;
import com.elefana.cluster.ClusterService;
import com.elefana.document.BulkIngestService;
import com.elefana.document.DocumentService;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.IndexTemplateService;
import com.elefana.node.NodesService;
import com.elefana.search.SearchService;

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
	private MetricRegistry metricRegistry;
	
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
