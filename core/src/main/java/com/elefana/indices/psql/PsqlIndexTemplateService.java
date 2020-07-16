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
package com.elefana.indices.psql;

import com.elefana.api.AckResponse;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.BadRequestException;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchTemplateException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.*;
import com.elefana.api.json.JsonUtils;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.IndexTemplateService;
import com.elefana.util.NamedThreadFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class PsqlIndexTemplateService implements IndexTemplateService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlIndexTemplateService.class);

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private final Map<String, String> indexToTemplateNullCache = new ConcurrentHashMap<String, String>();
	private final Map<String, String> indexToTemplateIdCache = new ConcurrentHashMap<String, String>();

	private final Map<String, IndexTemplate> templateIdToTemplateCache = new ConcurrentHashMap<String, IndexTemplate>();
	private final Map<String, String> templateIdToNullCache = new ConcurrentHashMap<String, String>();

	private ExecutorService executorService;
	
	@PostConstruct
	public void postConstruct() {
		final int totalThreads = environment.getProperty("elefana.service.template.threads", Integer.class, Runtime.getRuntime().availableProcessors());
		executorService = Executors.newFixedThreadPool(totalThreads, new NamedThreadFactory("elefana-indexTemplateService-requestExecutor"));
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();

		try {
			executorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}
	
	@Override
	public ListIndexTemplatesRequest prepareListIndexTemplates(String... templateIds) {
		return new PsqlListIndexTemplatesRequest(this, templateIds);
	}

	@Override
	public GetIndexTemplateRequest prepareGetIndexTemplate(String index, boolean fetchSource) {
		final PsqlGetIndexTemplateRequest request = new PsqlGetIndexTemplateRequest(this, index);
		request.setFetchSource(fetchSource);
		return request;
	}

	@Override
	public PutIndexTemplateRequest preparePutIndexTemplate(String templateId, PooledStringBuilder requestBody) {
		return new PsqlPutIndexTemplateRequest(this, templateId, requestBody);
	}
	
	@Override
	public GetIndexTemplateForIndexRequest prepareGetIndexTemplateForIndex(String index) {
		if (indexToTemplateNullCache.containsKey(index)) {
			return new ImmediateGetIndexTemplateForIndexRequest(index, null, null);
		}
		final String templateId = indexToTemplateIdCache.get(index);
		if (templateId != null && templateIdToTemplateCache.containsKey(templateId)) {
			return new ImmediateGetIndexTemplateForIndexRequest(index, templateId, templateIdToTemplateCache.get(templateId));
		}
		if (templateId != null && templateIdToNullCache.containsKey(templateId)) {
			return new ImmediateGetIndexTemplateForIndexRequest(index, templateId, null);
		}
		return new PsqlGetIndexTemplateForIndexRequest(this, index);
	}
	
	public Map<String, IndexTemplate> getIndexTemplates() {
		final Map<String, IndexTemplate> results = new HashMap<String, IndexTemplate>();
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_index_template");
			while (rowSet.next()) {
				final IndexTemplate indexTemplate = new IndexTemplate();
				indexTemplate.setTemplate(rowSet.getString("_index_pattern"));
				indexTemplate.setStorage(JsonUtils.fromJsonString(rowSet.getString("_storage"), IndexStorageSettings.class));
				indexTemplate.setMappings(JsonUtils.fromJsonString(rowSet.getString("_mappings"), Map.class));

				final String templateId = rowSet.getString("_template_id");
				results.put(templateId, indexTemplate);
				templateIdToTemplateCache.put(templateId, indexTemplate);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}

	public IndexTemplate getIndexTemplate(String templateId, boolean fetchSource) throws ElefanaException {
		if(fetchSource) {
			if(templateIdToTemplateCache.containsKey(templateId)) {
				return templateIdToTemplateCache.get(templateId);
			}
			if(templateIdToNullCache.containsKey(templateId)) {
				return null;
			}
		}
		final String query;

		if(fetchSource) {
			query = "SELECT * FROM elefana_index_template WHERE _template_id = ?";
		} else {
			query = "SELECT _index_pattern FROM elefana_index_template WHERE _template_id = ?";
		}

		SqlRowSet rowSet = jdbcTemplate.queryForRowSet(query, templateId);
		if (!rowSet.next()) {
			throw new NoSuchTemplateException(templateId);
		}
		try {
			final IndexTemplate result = new IndexTemplate();

			if(fetchSource) {
				result.setTemplate(rowSet.getString("_index_pattern"));
				result.setStorage(JsonUtils.fromJsonString(rowSet.getString("_storage"), IndexStorageSettings.class));
				result.setMappings(JsonUtils.fromJsonString(rowSet.getString("_mappings"), Map.class));
			} else {
				result.setTemplate(rowSet.getString("_index_pattern"));
				result.setMappings(new HashMap<String, Object>());
			}
			return result;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}
	
	public IndexTemplate getIndexTemplateForIndices(List<String> indices) throws ElefanaException {
		if(indices.isEmpty()) {
			return null;
		}
		IndexTemplate result = getIndexTemplateForIndex(indices.get(0));
		if(result == null) {
			return null;
		}
		for(int i = 1; i < indices.size(); i++) {
			final IndexTemplate nextIndexTemplate = getIndexTemplateForIndex(indices.get(i));
			if(nextIndexTemplate == null) {
				return null;
			}
			if(!nextIndexTemplate.getTemplate().equalsIgnoreCase(result.getTemplate())) {
				return null;
			}
		}
		return result;
	}

	public String getIndexTemplateIdForIndex(String index) throws ElefanaException {
		if (indexToTemplateNullCache.containsKey(index)) {
			return null;
		}
		String templateId = indexToTemplateIdCache.get(index);
		if (templateId != null) {
			return templateId;
		}

		templateId = internalGetIndexTemplateIdForIndex(index);
		if (templateId == null) {
			indexToTemplateNullCache.put(index, index);
			return null;
		}
		indexToTemplateIdCache.put(index, templateId);
		return templateId;
	}

	public IndexTemplate getIndexTemplateForIndex(String index) throws ElefanaException {
		if (indexToTemplateNullCache.containsKey(index)) {
			return null;
		}
		String templateId = indexToTemplateIdCache.get(index);
		if (templateId != null && templateIdToTemplateCache.containsKey(templateId)) {
			return templateIdToTemplateCache.get(templateId);
		}
		if (templateId != null && templateIdToNullCache.containsKey(templateId)) {
			return null;
		}

		templateId = internalGetIndexTemplateIdForIndex(index);
		if (templateId == null) {
			indexToTemplateNullCache.put(index, index);
			return null;
		}
		final IndexTemplate result = getIndexTemplate(templateId, true);
		if (result != null) {
			templateIdToTemplateCache.put(templateId, result);
			indexToTemplateIdCache.put(index, templateId);
		} else {
			templateIdToNullCache.put(templateId, templateId);
			indexToTemplateIdCache.put(index, templateId);
		}
		return result;
	}

	private String internalGetIndexTemplateIdForIndex(String index) {
		final Map<String, IndexTemplate> indexTemplates = getIndexTemplates();
		for (String templateId : indexTemplates.keySet()) {
			final IndexTemplate indexTemplate = indexTemplates.get(templateId);
			String indexRegex = indexTemplate.getTemplate().replace("*", "(.*)");
			indexRegex = "^" + indexRegex + "$";
			if (index.matches(indexRegex)) {
				return templateId;
			}
		}
		return null;
	}

	private IndexTemplate internalGetIndexTemplateForIndex(String index) {
		for (IndexTemplate indexTemplate : getIndexTemplates().values()) {
			String indexRegex = indexTemplate.getTemplate().replace("*", "(.*)");
			indexRegex = "^" + indexRegex + "$";
			if (index.matches(indexRegex)) {
				return indexTemplate;
			}
		}
		return null;
	}

	public AckResponse putIndexTemplate(String templateId, PooledStringBuilder requestBody) throws ElefanaException {
		final JsonNode templateData = JsonUtils.extractJsonNode(requestBody);
		if (!templateData.has("template")) {
			throw new BadRequestException();
		}

		try {
			final String indexPattern = templateData.get("template").textValue();

			PGobject storageObject = new PGobject();
			storageObject.setType("json");
			if(templateData.has("storage") && templateData.get("storage").isObject()) {
				storageObject.setValue(templateData.get("storage").toString());
			} else {
				storageObject.setValue("{}");
			}
			
			PGobject mappingsObject = new PGobject();
			mappingsObject.setType("json");
			if (templateData.has("mappings") && templateData.get("mappings").isObject()) {
				mappingsObject.setValue(templateData.get("mappings").toString());
			} else {
				mappingsObject.setValue("{}");
			}

			jdbcTemplate.update(
					"INSERT INTO elefana_index_template (_template_id, _index_pattern, _storage, _mappings) VALUES (?, ?, ?, ?) ON CONFLICT (_template_id) DO UPDATE SET _mappings = EXCLUDED._mappings, _storage = EXCLUDED._storage, _index_pattern = EXCLUDED._index_pattern",
					templateId, indexPattern, storageObject, mappingsObject);

			indexToTemplateIdCache.clear();
			indexToTemplateNullCache.clear();
			templateIdToTemplateCache.clear();
			templateIdToNullCache.clear();
			return new AckResponse();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
