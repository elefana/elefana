/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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
package com.elefana.indices;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.node.NodeSettingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@DependsOn("indexTemplateService")
public class IndexTemplateCache {
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	private final ConcurrentMap<String, IndexTemplate> cache = new ConcurrentHashMap<String, IndexTemplate>();

	private volatile long lastExpiry = 0L;

	public IndexTemplate getIndexTemplate(String index) throws ElefanaException {
		final long timestamp = System.currentTimeMillis();
		if(timestamp - lastExpiry >= nodeSettingsService.getIndexTemplateCacheExpiry()) {
			cache.clear();
			lastExpiry = timestamp;
		}

		final IndexTemplate result = cache.get(index);

		if(!cache.containsKey(index)) {
			final GetIndexTemplateForIndexRequest indexTemplateForIndexRequest = indexTemplateService
					.prepareGetIndexTemplateForIndex(index);
			final GetIndexTemplateForIndexResponse indexTemplateForIndexResponse = indexTemplateForIndexRequest.get();
			final IndexTemplate indexTemplate = indexTemplateForIndexResponse.getIndexTemplate();
			cache.put(index, indexTemplate);
			return indexTemplate;
		}
		return result;
	}
}
