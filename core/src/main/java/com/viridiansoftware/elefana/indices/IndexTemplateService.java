/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.indices;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.viridiansoftware.elefana.exception.BadRequestException;
import com.viridiansoftware.elefana.exception.NoSuchTemplateException;
import com.viridiansoftware.elefana.exception.ShardFailedException;
import com.viridiansoftware.elefana.node.NodeSettingsService;
import com.viridiansoftware.elefana.node.VersionInfoService;
import com.viridiansoftware.elefana.util.TableUtils;

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
	private TableUtils tableUtils;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	@PostConstruct
	public void postConstruct() {
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_template (_template_id VARCHAR(255) PRIMARY KEY, _index_pattern VARCHAR(255), _table_pattern VARCHAR(255), _mappings jsonb);");
		
		if (nodeSettingsService.isUsingCitus()) {
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_template', '_template_id');");
		}
	}
	
	public List<IndexTemplate> getIndexTemplates() {
		final List<IndexTemplate> results = new ArrayList<IndexTemplate>();
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM es2pgsql_index_template");
			while (rowSet.next()) {
				IndexTemplate indexTemplate = new IndexTemplate();
				indexTemplate.setTemplate(rowSet.getString("_index_pattern"));
				indexTemplate.setTableTemplate(rowSet.getString("_table_pattern"));
				indexTemplate.setMappings(objectMapper.readValue(rowSet.getString("_mappings"), Map.class));
				results.add(indexTemplate);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}
	
	public IndexTemplate getIndexTemplate(String templateId) {
		SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM es2pgsql_index_template WHERE _template_id = ?", templateId);
		if(!rowSet.next()) {
			throw new NoSuchTemplateException();
		}
		try {
			IndexTemplate result = new IndexTemplate();
			result.setTemplate(rowSet.getString("_index_pattern"));
			result.setTableTemplate(rowSet.getString("_table_pattern"));
			result.setMappings(objectMapper.readValue(rowSet.getString("_mappings"), Map.class));
			return result;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}
	
	public IndexTemplate getIndexTemplateForTableName(String tableName) {
		for(IndexTemplate indexTemplate : getIndexTemplates()) {
			LOGGER.info(tableName + " " + indexTemplate.toString());
			if(tableName.matches(indexTemplate.getTableTemplate())) {
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
			String indexPattern = templateData.get("template").toString();
			String tablePattern = TableUtils.sanitizeTableName(indexPattern).toLowerCase();
			tablePattern = tablePattern.replace("*", "(.*)");
			tablePattern = "^" + tablePattern + "$";
			
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(templateData.get("mappings").toString());

			jdbcTemplate.update(
					"INSERT INTO es2pgsql_index_template (_template_id, _index_pattern, _table_pattern, _mappings) VALUES (?, ?, ?, ?) ON CONFLICT (_template_id) DO UPDATE SET _mappings = EXCLUDED._mappings, _index_pattern = EXCLUDED._index_pattern, _table_pattern = EXCLUDED._table_pattern",
					templateId, indexPattern, tablePattern, jsonObject);
			return;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		throw new ShardFailedException();
	}
}
