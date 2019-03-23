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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.elefana.api.AckResponse;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.BadRequestException;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.NoSuchTemplateException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateRequest;
import com.elefana.api.indices.IndexStorageSettings;
import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.ListIndexTemplatesRequest;
import com.elefana.api.indices.PutIndexTemplateRequest;
import com.elefana.indices.IndexTemplateService;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.TypeLiteral;

@Service
public class PsqlIndexTemplateService implements IndexTemplateService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlIndexTemplateService.class);

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private final Map<String, String> indexToIndexTemplateNullCache = new ConcurrentHashMap<String, String>();
	private final Map<String, IndexTemplate> indexToIndexTemplateCache = new ConcurrentHashMap<String, IndexTemplate>();

	private ExecutorService executorService;
	
	@PostConstruct
	public void postConstruct() {
		final int totalThreads = environment.getProperty("elefana.service.template.threads", Integer.class, Runtime.getRuntime().availableProcessors());
		executorService = Executors.newFixedThreadPool(totalThreads);
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
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
	public PutIndexTemplateRequest preparePutIndexTemplate(String templateId, String requestBody) {
		return new PsqlPutIndexTemplateRequest(this, templateId, requestBody);
	}
	
	@Override
	public GetIndexTemplateForIndexRequest prepareGetIndexTemplateForIndex(String index) {
		if (indexToIndexTemplateNullCache.containsKey(index)) {
			return new ImmediateGetIndexTemplateForIndexRequest(index, null);
		}
		if (indexToIndexTemplateCache.containsKey(index)) {
			return new ImmediateGetIndexTemplateForIndexRequest(index, indexToIndexTemplateCache.get(index));
		}
		return new PsqlGetIndexTemplateForIndexRequest(this, index);
	}
	
	public List<IndexTemplate> getIndexTemplates() {
		final List<IndexTemplate> results = new ArrayList<IndexTemplate>();
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_index_template");
			while (rowSet.next()) {
				IndexTemplate indexTemplate = new IndexTemplate(rowSet.getString("_template_id"));
				indexTemplate.setTemplate(rowSet.getString("_index_pattern"));
				indexTemplate.setStorage(JsonIterator.deserialize(rowSet.getString("_storage"), IndexStorageSettings.class));
				indexTemplate.setMappings(
						JsonIterator.deserialize(rowSet.getString("_mappings"), new TypeLiteral<Map<String, Object>>() {
						}));
				results.add(indexTemplate);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}

	public IndexTemplate getIndexTemplate(String templateId, boolean fetchSource) throws ElefanaException {
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
			IndexTemplate result = new IndexTemplate(templateId);

			if(fetchSource) {
				result.setTemplate(rowSet.getString("_index_pattern"));
				result.setStorage(JsonIterator.deserialize(rowSet.getString("_storage"), IndexStorageSettings.class));
				result.setMappings(
						JsonIterator.deserialize(rowSet.getString("_mappings"), new TypeLiteral<Map<String, Object>>() {
						}));
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
	
	public IndexTemplate getIndexTemplateForIndices(List<String> indices) {
		if(indices.isEmpty()) {
			return null;
		}
		IndexTemplate result = getIndexTemplateForIndex(indices.get(0));
		if(result == null) {
			return null;
		}
		for(int i = 1; i < indices.size(); i++) {
			IndexTemplate nextIndexTemplate = getIndexTemplateForIndex(indices.get(i));
			if(nextIndexTemplate == null) {
				return null;
			}
			if(!nextIndexTemplate.getTemplateId().equalsIgnoreCase(result.getTemplateId())) {
				return null;
			}
		}
		return result;
	}

	public IndexTemplate getIndexTemplateForIndex(String index) {
		if (indexToIndexTemplateNullCache.containsKey(index)) {
			return null;
		}
		if (indexToIndexTemplateCache.containsKey(index)) {
			return indexToIndexTemplateCache.get(index);
		}

		IndexTemplate result = internalGetIndexTemplateForIndex(index);
		if (result != null) {
			indexToIndexTemplateCache.put(index, result);
		} else {
			indexToIndexTemplateNullCache.put(index, index);
		}
		return result;
	}

	private IndexTemplate internalGetIndexTemplateForIndex(String index) {
		for (IndexTemplate indexTemplate : getIndexTemplates()) {
			String indexRegex = indexTemplate.getTemplate().replace("*", "(.*)");
			indexRegex = "^" + indexRegex + "$";
			if (index.matches(indexRegex)) {
				return indexTemplate;
			}
		}
		return null;
	}

	public AckResponse putIndexTemplate(String templateId, String requestBody) throws ElefanaException {
		Any templateData = JsonIterator.deserialize(requestBody);
		if (templateData.get("template").valueType().equals(ValueType.INVALID)) {
			throw new BadRequestException();
		}

		try {
			final String indexPattern = templateData.get("template").toString();

			PGobject storageObject = new PGobject();
			storageObject.setType("json");
			if(templateData.get("storage").valueType().equals(ValueType.OBJECT)) {
				storageObject.setValue(templateData.get("storage").toString());
			} else {
				storageObject.setValue("{}");
			}
			
			PGobject mappingsObject = new PGobject();
			mappingsObject.setType("json");
			if (templateData.get("mappings").valueType().equals(ValueType.OBJECT)) {
				mappingsObject.setValue(templateData.get("mappings").toString());
			} else {
				mappingsObject.setValue("{}");
			}

			jdbcTemplate.update(
					"INSERT INTO elefana_index_template (_template_id, _index_pattern, _storage, _mappings) VALUES (?, ?, ?, ?) ON CONFLICT (_template_id) DO UPDATE SET _mappings = EXCLUDED._mappings, _storage = EXCLUDED._storage, _index_pattern = EXCLUDED._index_pattern",
					templateId, indexPattern, storageObject, mappingsObject);

			indexToIndexTemplateNullCache.clear();
			indexToIndexTemplateCache.clear();
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
