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
package com.elefana.indices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.elefana.exception.BadRequestException;
import com.elefana.exception.NoSuchTemplateException;
import com.elefana.exception.ShardFailedException;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;

@Service
public class IndexTemplateService {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexTemplateService.class);
	
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	public List<IndexTemplate> getIndexTemplates() {
		final List<IndexTemplate> results = new ArrayList<IndexTemplate>();
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_index_template");
			while (rowSet.next()) {
				IndexTemplate indexTemplate = new IndexTemplate();
				indexTemplate.setTemplate(rowSet.getString("_index_pattern"));
				indexTemplate.setTimestamp_path(rowSet.getString("_timestamp_path"));
				indexTemplate.setMappings(objectMapper.readValue(rowSet.getString("_mappings"), Map.class));
				results.add(indexTemplate);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}
	
	public IndexTemplate getIndexTemplate(String templateId) {
		SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM elefana_index_template WHERE _template_id = ?", templateId);
		if(!rowSet.next()) {
			throw new NoSuchTemplateException();
		}
		try {
			IndexTemplate result = new IndexTemplate();
			result.setTemplate(rowSet.getString("_index_pattern"));
			result.setTimestamp_path(rowSet.getString("_timestamp_path"));
			result.setMappings(objectMapper.readValue(rowSet.getString("_mappings"), Map.class));
			return result;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}
	
	public IndexTemplate getIndexTemplateForIndex(String index) {
		for(IndexTemplate indexTemplate : getIndexTemplates()) {
			String indexRegex = indexTemplate.getTemplate().replace("*", "(.*)");
			indexRegex = "^" + indexRegex + "$";
			if(index.matches(indexRegex)) {
				return indexTemplate;
			}
		}
		return null;
	}
	
	public void putIndexTemplate(String templateId, String requestBody) {
		Any templateData = JsonIterator.deserialize(requestBody);
		if(templateData.get("template").valueType().equals(ValueType.INVALID)) {
			throw new BadRequestException();
		}
		if(templateData.get("mappings").valueType().equals(ValueType.INVALID)) {
			throw new BadRequestException();
		}
		
		try {
			final String indexPattern = templateData.get("template").toString();
			
			String timestampPath = null;
			
			if(templateData.get("timestamp_path").valueType().equals(ValueType.STRING)) {
				timestampPath = templateData.get("timestamp_path").toString();
			}
			
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(templateData.get("mappings").toString());

			jdbcTemplate.update(
					"INSERT INTO elefana_index_template (_template_id, _index_pattern, _timestamp_path, _mappings) VALUES (?, ?, ?, ?) ON CONFLICT (_template_id) DO UPDATE SET _mappings = EXCLUDED._mappings, _index_pattern = EXCLUDED._index_pattern",
					templateId, indexPattern, timestampPath, jsonObject);
			return;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}
}
