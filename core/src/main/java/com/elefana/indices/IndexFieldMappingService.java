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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import com.elefana.ApiVersion;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class IndexFieldMappingService implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMappingService.class);

	private final SortedSet<String> mappingQueue = new ConcurrentSkipListSet<String>();
	private final SortedSet<String> fieldStatsQueue = new ConcurrentSkipListSet<String>();
	private long lastMapping = -1L;
	private long lastFieldStats = -1L;

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;
	@Autowired
	private IndexTemplateService indexTemplateService;
	@Autowired
	private TaskScheduler taskScheduler;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private FieldMapper fieldMapper;

	@PostConstruct
	public void postConstruct() {
		switch (versionInfoService.getApiVersion()) {
		case V_2_4_3:
			fieldMapper = new V2FieldMapper();
			break;
		case V_5_5_2:
		default:
			fieldMapper = new V5FieldMapper();
			break;
		}

		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_mapping (_tracking_id VARCHAR(255) PRIMARY KEY, _index VARCHAR(255), _type VARCHAR(255), _mapping jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_capabilities (_index VARCHAR(255) PRIMARY KEY, _capabilities jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS elefana_index_field_stats (_index VARCHAR(255) PRIMARY KEY, _stats jsonb);");
		if (nodeSettingsService.isUsingCitus()) {
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_mapping', '_tracking_id');");
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_capabilities', '_index');");
			jdbcTemplate.execute("SELECT create_distributed_table('elefana_index_field_stats', '_index');");
		}

		taskScheduler.scheduleWithFixedDelay(this,
				Math.min(nodeSettingsService.getFieldStatsInterval(), nodeSettingsService.getMappingInterval()));
	}

	public List<String> getFieldNames(String index, String type) {
		return fieldMapper.getFieldNames(getIndexTypeMapping(index, type));
	}

	public List<String> getTypesForIndex(String index) {
		return jdbcTemplate.queryForList("SELECT _type FROM elefana_index_mapping WHERE _index = ?", String.class,
				index);
	}

	public List<String> getTypesForIndex(String index, String typePattern) {
		String[] typePatterns = typePattern.split(",");

		List<String> results = new ArrayList<String>(1);
		List<String> sqlResults = getTypesForIndex(index);

		for (String type : sqlResults) {
			for (String pattern : typePatterns) {
				pattern = pattern.replace(".", "\\$");
				pattern = pattern.replace("*", "(.*)");
				pattern = "^" + pattern + "$";

				if (type.matches(pattern)) {
					results.add(type);
					break;
				}
			}
		}
		return results;
	}

	public Map<String, Object> getMappings() throws Exception {
		return getMapping("*");
	}

	public Map<String, Object> getMapping(String indexPattern) throws Exception {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			Map<String, Object> mappings = new HashMap<String, Object>();
			mappings.put("mappings", getIndexTypeMappings(index));

			result.put(index, mappings);
		}
		return result;
	}

	public Map<String, Object> getMapping(String indexPattern, String type) {
		Map<String, Object> mappings = new HashMap<String, Object>();
		mappings.put(type, getIndexTypeMapping(indexPattern, type));

		Map<String, Object> indexTypeMappings = new HashMap<String, Object>();
		indexTypeMappings.put("mappings", mappings);

		Map<String, Object> result = new HashMap<String, Object>();
		result.put(indexPattern, indexTypeMappings);
		return result;
	}

	public void putMapping(String index, String mappingBody) throws Exception {
		Map<String, Object> mappings = (Map<String, Object>) objectMapper.readValue(mappingBody, Map.class)
				.get("mappings");
		if (mappings == null) {
			return;
		}
		for (String type : mappings.keySet()) {
			putIndexMapping(index, type, (Map<String, Object>) mappings.get(type));
		}
	}

	public void putMapping(String index, String type, String mappingBody) throws Exception {
		putIndexMapping(index, type, objectMapper.readValue(mappingBody, Map.class));
	}

	private void putIndexMapping(String index, String type, Map<String, Object> newMappings) throws Exception {
		Map<String, Object> existingMappings = getIndexTypeMapping(index, type);

		if (newMappings.containsKey(type)) {
			newMappings = (Map<String, Object>) newMappings.get(type);
		}
		if (newMappings.containsKey("properties")) {
			newMappings = (Map<String, Object>) newMappings.get("properties");
		}
		for (String propertyName : newMappings.keySet()) {
			if (!existingMappings.containsKey(propertyName)) {
				existingMappings.put(propertyName, fieldMapper.getEmptyMapping());
			}
			fieldMapper.mergeMapping((Map<String, Object>) existingMappings.get(propertyName), propertyName,
					(Map<String, Object>) newMappings.get(propertyName));
		}
		saveMappings(index, type, existingMappings);
		generateFieldCapabilitiesForIndex(index);
		scheduleIndexForStats(index);
	}

	public Map<String, Object> getMapping(String index, String typePattern, String field) {
		Map<String, Object> mappings = new HashMap<String, Object>();

		Map<String, Object> indexMapping = new HashMap<String, Object>();
		indexMapping.put("mappings", mappings);

		Map<String, Object> result = new HashMap<String, Object>();
		result.put(index, indexMapping);

		for (String type : getTypesForIndex(index, typePattern)) {
			Map<String, Object> typeMappings = getIndexTypeMapping(index, type);

			if (versionInfoService.getApiVersion().isNewerThan(ApiVersion.V_2_4_3)) {
				Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");

				Map<String, Object> mapping = new HashMap<String, Object>();
				if (properties.containsKey(field)) {
					mapping.put(field, properties.get(field));
				}

				Map<String, Object> fieldMapping = new HashMap<String, Object>();
				fieldMapping.put("full_name", field);
				fieldMapping.put("mapping", mapping);

				Map<String, Object> typeFieldMapping = new HashMap<String, Object>();
				typeFieldMapping.put(field, fieldMapping);

				mappings.put(type, typeFieldMapping);
			} else {
				Map<String, Object> typeFieldMapping = new HashMap<String, Object>();
				typeFieldMapping.put(field, typeMappings.get(field));
				mappings.put(type, typeFieldMapping);
			}
		}
		return result;
	}

	public String getFirstFieldMappingType(List<String> indices, String[] typePatterns, String field) {
		for (String index : indices) {
			if (IndexUtils.isTypesEmpty(typePatterns)) {
				String result = getFirstFieldMappingType(index, "*", field);
				if (result != null) {
					return result;
				}
			} else {
				for (String typePattern : typePatterns) {
					String result = getFirstFieldMappingType(index, typePattern, field);
					if (result != null) {
						return result;
					}
				}
			}
		}
		return null;
	}

	public String getFirstFieldMappingType(String index, String typePattern, String field) {
		for (String type : getTypesForIndex(index, typePattern)) {
			Map<String, Object> typeMappings = getIndexTypeMapping(index, type);

			switch (versionInfoService.getApiVersion()) {
			case V_2_4_3: {
				if (!typeMappings.containsKey(field)) {
					return null;
				}
				Map<String, Object> fieldMapping = (Map<String, Object>) typeMappings.get(field);
				Map<String, Object> mapping = (Map<String, Object>) fieldMapping.get("mapping");
				Map<String, Object> mappingFieldMapping = (Map<String, Object>) mapping.get(field);
				return (String) mappingFieldMapping.get("type");
			}
			case V_5_5_2:
			default: {
				Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");
				if (properties.containsKey(field)) {
					Map<String, Object> property = (Map) properties.get(field);
					return (String) property.get("type");
				}
				break;
			}
			}
		}
		return null;
	}

	public String getFirstFieldMappingFormat(List<String> indices, String[] typePatterns, String field) {
		for (String index : indices) {
			if (IndexUtils.isTypesEmpty(typePatterns)) {
				String result = getFirstFieldMappingFormat(index, "*", field);
				if (result != null) {
					return result;
				}
			} else {
				for (String typePattern : typePatterns) {
					String result = getFirstFieldMappingFormat(index, typePattern, field);
					if (result != null) {
						return result;
					}
				}
			}
		}
		return null;
	}

	public String getFirstFieldMappingFormat(String index, String typePattern, String field) {
		for (String type : getTypesForIndex(index, typePattern)) {
			Map<String, Object> typeMappings = getIndexTypeMapping(index, type);

			switch (versionInfoService.getApiVersion()) {
			case V_2_4_3: {
				if (!typeMappings.containsKey(field)) {
					return null;
				}
				Map<String, Object> fieldMapping = (Map<String, Object>) typeMappings.get(field);
				Map<String, Object> mapping = (Map<String, Object>) fieldMapping.get("mapping");
				Map<String, Object> mappingFieldMapping = (Map<String, Object>) mapping.get(field);
				if (mappingFieldMapping.containsKey("format")) {
					return (String) mappingFieldMapping.get("format");
				}
				return (String) mappingFieldMapping.get("type");
			}
			case V_5_5_2:
			default: {
				Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");
				if (properties.containsKey(field)) {
					Map<String, Object> property = (Map) properties.get(field);
					if (property.containsKey("format")) {
						return (String) property.get("format");
					}
					return (String) property.get("type");
				}
				break;
			}
			}
		}
		return null;
	}

	public Map<String, Object> getFieldStats(String indexPattern) throws Exception {
		final Map<String, Object> shards = new HashMap<String, Object>();
		shards.put("total", 1);
		shards.put("successful", 1);
		shards.put("failed", 0);

		final Map<String, Object> results = new HashMap<String, Object>();
		results.put("_shards", shards);

		final Map<String, Object> indicesResults = new HashMap<String, Object>();
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			final Map<String, Object> indexResult = new HashMap<String, Object>();

			try {
				SqlRowSet rowSet = jdbcTemplate
						.queryForRowSet("SELECT _stats FROM elefana_index_field_stats WHERE _index = ? LIMIT 1", index);
				if (rowSet.next()) {
					indexResult.put("fields", objectMapper.readValue(rowSet.getString("_stats"), Map.class));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			indicesResults.put(index, indexResult);
		}
		results.put("indices", indicesResults);
		return results;
	}

	public Map<String, Object> getFieldCapabilities(String indexPattern) throws Exception {
		Map<String, Object> fields = new HashMap<String, Object>();
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			try {
				SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
						"SELECT _capabilities FROM elefana_index_field_capabilities WHERE _index = ? LIMIT 1", index);
				if (rowSet.next()) {
					fields.putAll(objectMapper.readValue(rowSet.getString("_capabilities"), Map.class));
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("fields", fields);
		return result;
	}

	private Map<String, Object> getIndexTypeMappings(String index) {
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			SqlRowSet rowSet = jdbcTemplate
					.queryForRowSet("SELECT _type, _mapping FROM elefana_index_mapping WHERE _index = ?", index);
			while (rowSet.next()) {
				results.put(rowSet.getString("_type"), objectMapper.readValue(rowSet.getString("_mapping"), Map.class));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}

	private Map<String, Object> getIndexTypeMapping(String index, String type) {
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
					"SELECT _mapping FROM elefana_index_mapping WHERE _index = ? AND _type = ? LIMIT 1", index, type);
			if (rowSet.next()) {
				return objectMapper.readValue(rowSet.getString("_mapping"), Map.class);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return new HashMap<String, Object>(fieldMapper.getEmptyMapping());
	}

	@Override
	public void run() {
		generateMappingsForQueuedTables();
		generateFieldStatsForQueuedTables();
	}

	private void generateMappingsForQueuedTables() {
		if (System.currentTimeMillis() - lastMapping < nodeSettingsService.getMappingInterval()) {
			return;
		}
		try {
			while (!mappingQueue.isEmpty()) {
				String nextIndex = mappingQueue.first();

				Map<String, Object> mapping = getIndexTypeMappings(nextIndex);

				if (mapping.isEmpty()) {
					IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(nextIndex);

					SqlRowSet rowSet = getSampleDocuments(nextIndex);
					int totalSamples = generateMappingsForAllTypes(indexTemplate, mapping, nextIndex, rowSet);
					if (totalSamples == 0) {
						rowSet = jdbcTemplate.queryForRowSet("SELECT _type, _source FROM " + IndexUtils.DATA_TABLE
								+ " WHERE _index = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(),
								nextIndex);
						generateMappingsForAllTypes(indexTemplate, mapping, nextIndex, rowSet);
					}
				} else {
					for (String type : mapping.keySet()) {
						Map<String, Object> typeMappings = (Map) mapping.get(type);
						SqlRowSet rowSet = getSampleDocuments(nextIndex, type);
						int totalSamples = generateMappingsForType(typeMappings, nextIndex, type, rowSet);
						if (totalSamples == 0) {
							rowSet = jdbcTemplate.queryForRowSet("SELECT _source FROM " + IndexUtils.DATA_TABLE
									+ " WHERE _index = ? AND _type = ? LIMIT "
									+ nodeSettingsService.getFallbackMappingSampleSize(), nextIndex, type);
							generateMappingsForType(typeMappings, nextIndex, type, rowSet);
						}
					}
				}
				generateFieldCapabilitiesForIndex(nextIndex);

				mappingQueue.remove(nextIndex);
			}
			lastMapping = System.currentTimeMillis();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private int generateMappingsForAllTypes(IndexTemplate indexTemplate, Map<String, Object> mapping, String index,
			SqlRowSet rowSet) throws Exception {
		int totalSamples = 0;
		while (rowSet.next()) {
			final String type = rowSet.getString("_type");

			Map<String, Object> typeMappings = (Map) mapping.get(type);
			if (typeMappings == null) {
				if (indexTemplate != null) {
					typeMappings = fieldMapper.convertIndexTemplateToMappings(indexTemplate, type);
				}
				if (typeMappings == null) {
					typeMappings = new HashMap<String, Object>();
				}
				mapping.put(type, typeMappings);
			}
			Map<String, Object> document = objectMapper.readValue(rowSet.getString("_source"), Map.class);
			fieldMapper.generateMappings(typeMappings, document);

			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(objectMapper.writeValueAsString(typeMappings));

			jdbcTemplate.update(
					"INSERT INTO elefana_index_mapping (_tracking_id, _index, _type, _mapping) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
					index + "-" + type, index, type, jsonObject);
			totalSamples++;
		}
		return totalSamples;
	}

	private int generateMappingsForType(Map<String, Object> typeMappings, String nextTable, String type,
			SqlRowSet rowSet) throws Exception {
		int totalSamples = 0;
		while (rowSet.next()) {
			Map<String, Object> document = objectMapper.readValue(rowSet.getString("_source"), Map.class);
			fieldMapper.generateMappings(typeMappings, document);
			totalSamples++;
		}

		if (totalSamples == 0) {
			return 0;
		}

		saveMappings(nextTable, type, typeMappings);
		return totalSamples;
	}

	private void generateFieldStatsForQueuedTables() {
		if (System.currentTimeMillis() - lastFieldStats < nodeSettingsService.getFieldStatsInterval()) {
			return;
		}
		try {
			while (!fieldStatsQueue.isEmpty()) {
				String nextIndex = fieldStatsQueue.first();
				generateFieldStatsForIndex(nextIndex);
				fieldStatsQueue.remove(nextIndex);
			}
			lastFieldStats = System.currentTimeMillis();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void generateFieldStatsForIndex(String indexName) throws Exception {
		final Map<String, V2FieldStats> fieldStats = new HashMap<String, V2FieldStats>();
		final List<String> types = getTypesForIndex(indexName);

		long totalDocs = (long) jdbcTemplate
				.queryForList("SELECT COUNT(*) FROM " + IndexUtils.DATA_TABLE + " WHERE _index='" + indexName + "'")
				.get(0).get("count");

		for (String type : types) {
			List<String> fieldNames = getFieldNames(indexName, type);

			for (String fieldName : fieldNames) {
				if (fieldStats.containsKey(fieldName)) {
					continue;
				}
				long totalDocsWithField = (long) jdbcTemplate.queryForList("SELECT COUNT(*) FROM "
						+ IndexUtils.DATA_TABLE + " WHERE _index='" + indexName + "' AND _source ? '" + fieldName + "'")
						.get(0).get("count");

				V2FieldStats stats = versionInfoService.getApiVersion().isNewerThan(ApiVersion.V_2_4_3)
						? new V5FieldStats() : new V2FieldStats();
				stats.max_doc = totalDocs;
				stats.doc_count = totalDocsWithField;
				stats.density = (int) ((stats.doc_count / (stats.max_doc * 1.0)) * 100.0);
				stats.sum_doc_freq = -1;
				stats.sum_total_term_freq = -1;

				fieldStats.put(fieldName, stats);
			}
		}

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(objectMapper.writeValueAsString(fieldStats));

		jdbcTemplate.update(
				"INSERT INTO elefana_index_field_stats (_index, _stats) VALUES (?, ?) ON CONFLICT (_index) DO UPDATE SET _stats = EXCLUDED._stats",
				indexName, jsonObject);
	}

	private void generateFieldCapabilitiesForIndex(String index) throws Exception {
		generateFieldCapabilitiesForIndex(index, jdbcTemplate
				.queryForRowSet("SELECT _type, _mapping FROM elefana_index_mapping WHERE _index = ?", index));
	}

	private void generateFieldCapabilitiesForIndex(String index, SqlRowSet indexMappingSet) throws Exception {
		if (!versionInfoService.getApiVersion().isNewerThan(ApiVersion.V_2_4_3)) {
			return;
		}

		Map<String, Object> fields = new HashMap<String, Object>();

		while (indexMappingSet.next()) {
			Map<String, Object> mappings = objectMapper.readValue(indexMappingSet.getString("_mapping"), Map.class);
			Map<String, Object> properties = (Map) mappings.get("properties");
			for (String propertyKey : properties.keySet()) {
				if (fields.containsKey(propertyKey)) {
					continue;
				}
				String mappingType = (String) (((Map) properties.get(propertyKey)).get("type"));

				Map<String, Object> field = new HashMap<String, Object>();
				field.put(mappingType, generateFieldCapability(mappingType));
				fields.put(propertyKey, field);
			}
		}

		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(objectMapper.writeValueAsString(fields));
		jdbcTemplate.update(
				"INSERT INTO elefana_index_field_capabilities (_index, _capabilities) VALUES (?, ?) ON CONFLICT (_index) DO UPDATE SET _capabilities = EXCLUDED._capabilities",
				index, jsonObject);
	}

	private Map<String, Object> generateFieldCapability(String mappingType) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("searchable", true);
		result.put("aggregatable", true);
		result.put("indices", null);
		result.put("non_searchable_indices", null);
		result.put("non_aggregatable_indices", null);
		return result;
	}

	private SqlRowSet getSampleDocuments(String index) {
		if (nodeSettingsService.isUsingCitus()) {
			return jdbcTemplate.queryForRowSet("SELECT _type, _source FROM " + IndexUtils.DATA_TABLE
					+ " WHERE _index = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(), index);
		} else {
			return jdbcTemplate.queryForRowSet(
					"SELECT _type, _source FROM " + IndexUtils.DATA_TABLE + " TABLESAMPLE BERNOULLI("
							+ String.format("%f", nodeSettingsService.getMappingSampleSize()) + ") WHERE _index = ?",
					index);
		}
	}

	private SqlRowSet getSampleDocuments(String index, String type) {
		if (nodeSettingsService.isUsingCitus()) {
			return jdbcTemplate.queryForRowSet("SELECT _source FROM " + IndexUtils.DATA_TABLE
					+ " WHERE _index = ? AND _type = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(),
					index, type);
		} else {
			return jdbcTemplate.queryForRowSet("SELECT _source FROM " + IndexUtils.DATA_TABLE
					+ " TABLESAMPLE BERNOULLI(" + String.format("%f", nodeSettingsService.getMappingSampleSize())
					+ ") WHERE _index = ? AND _type = ?", index, type);
		}
	}

	public void scheduleIndexForStats(String index) {
		fieldStatsQueue.add(index);
	}

	public void scheduleIndexForMapping(String index) {
		mappingQueue.add(index);
	}

	public void scheduleIndexForMappingAndStats(String index) {
		mappingQueue.add(index);
		fieldStatsQueue.add(index);
	}

	private void saveMappings(String index, String type, Map<String, Object> mappings) throws Exception {
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(objectMapper.writeValueAsString(mappings));

		jdbcTemplate.update(
				"INSERT INTO elefana_index_mapping (_tracking_id, _index, _type, _mapping) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
				index + "-" + type, index, type, jsonObject);
	}
}