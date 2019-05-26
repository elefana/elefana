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

import com.elefana.ApiVersion;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.*;
import com.elefana.indices.*;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.TypeLiteral;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

@Service
@DependsOn("nodeSettingsService")
public class PsqlIndexFieldMappingService implements IndexFieldMappingService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMappingService.class);

	private long lastMapping = -1L;
	private long lastFieldStats = -1L;

	@Autowired
	private Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;
	@Autowired
	private PsqlIndexTemplateService indexTemplateService;
	@Autowired
	private TaskScheduler taskScheduler;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private IndexUtils indexUtils;

	private IndexMappingQueue mappingQueue;
	private IndexFieldStatsQueue fieldStatsQueue;

	private ExecutorService executorService;
	private ScheduledFuture<?> mappingScheduledTask, fieldStatsScheduledTask;

	private FieldMapper fieldMapper;

	@PostConstruct
	public void postConstruct() throws SQLException {
		switch (versionInfoService.getApiVersion()) {
		case V_2_4_3:
			fieldMapper = new V2FieldMapper();
			break;
		case V_5_5_2:
		default:
			fieldMapper = new V5FieldMapper();
			break;
		}

		mappingQueue = new IndexMappingQueue(jdbcTemplate, taskScheduler, environment.getProperty("elefana.service.field.queue.io.interval", Long.class, TimeUnit.MINUTES.toMillis(15L)));
		fieldStatsQueue = new IndexFieldStatsQueue(jdbcTemplate, taskScheduler, environment.getProperty("elefana.service.field.queue.io.interval", Long.class, TimeUnit.MINUTES.toMillis(15L)));

		final int totalThreads = environment.getProperty("elefana.service.field.threads", Integer.class, 2);
		executorService = Executors.newFixedThreadPool(totalThreads);

		mappingScheduledTask = taskScheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					generateMappingsForQueuedTables();
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}, nodeSettingsService.getMappingInterval());
		fieldStatsScheduledTask = taskScheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					generateFieldStatsForQueuedTables();
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}, nodeSettingsService.getFieldStatsInterval());
	}

	@PreDestroy
	public void preDestroy() {
		if (mappingScheduledTask != null) {
			mappingScheduledTask.cancel(false);
		}
		if (fieldStatsScheduledTask != null) {
			fieldStatsScheduledTask.cancel(false);
		}
		executorService.shutdown();
	}

	public List<String> getFieldNamesByMapping(String index, String type) {
		return fieldMapper.getFieldNamesFromMapping(getIndexTypeMapping(index, type));
	}

	public Set<String> getFieldNames(String index, String type) {
		final Set<String> results = new HashSet<String>();
		try {
			SqlRowSet rowSet = jdbcTemplate
					.queryForRowSet("SELECT _field_names FROM elefana_index_field_names WHERE _index = ? AND _type = ?", index, type);
			while (rowSet.next()) {
				results.addAll(JsonIterator.deserialize(rowSet.getString("_field_names"), new TypeLiteral<List<String>>(){}));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
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

	public GetFieldMappingsResponse getMappings() throws ElefanaException {
		return getMapping("*");
	}

	public GetFieldMappingsResponse getMapping(String indexPattern) throws ElefanaException {
		GetFieldMappingsResponse result = new GetFieldMappingsResponse();
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			Map<String, Object> mappings = new HashMap<String, Object>();
			mappings.put("mappings", getIndexTypeMappings(index));

			result.getIndicesMappings().put(index, mappings);
		}
		return result;
	}

	public GetFieldMappingsResponse getMapping(String indexPattern, String type) throws ElefanaException {
		GetFieldMappingsResponse result = new GetFieldMappingsResponse();
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			Map<String, Object> typeMapping = new HashMap<String, Object>();
			typeMapping.put(type, getIndexTypeMapping(indexPattern, type));
			
			Map<String, Object> mappings = new HashMap<String, Object>();
			mappings.put("mappings", typeMapping);

			result.getIndicesMappings().put(index, mappings);
		}
		return result;
	}

	public void putMapping(String index, String mappingBody) throws ElefanaException {
		Map<String, Object> mappings = (Map<String, Object>) JsonIterator.deserialize(mappingBody, new TypeLiteral<Map<String, Object>>(){}).get("mappings");
		if (mappings == null) {
			return;
		}
		for (String type : mappings.keySet()) {
			putIndexMapping(index, type, (Map<String, Object>) mappings.get(type));
		}
	}

	public void putMapping(String index, String type, String mappingBody) throws ElefanaException {
		putIndexMapping(index, type, JsonIterator.deserialize(mappingBody, new TypeLiteral<Map<String, Object>>(){}));
	}

	private void putIndexMapping(String index, String type, Map<String, Object> newMappings) throws ElefanaException {
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
	
	public GetFieldMappingsResponse getFieldMapping(String indexPattern, String typePattern, String fieldPattern) throws ElefanaException {
		final List<String> matchedIndices = indexUtils.listIndicesForIndexPattern(indexPattern);
		final List<String> matchedFields = new ArrayList<String>();
		
		fieldPattern = fieldPattern.toLowerCase();
		fieldPattern = fieldPattern.replace("-", "\\-");
		fieldPattern = fieldPattern.replace("*", "(.*)");
		fieldPattern = "^" + fieldPattern + "$";
		
		final GetFieldMappingsResponse result = new GetFieldMappingsResponse();
		
		for (String index : matchedIndices) {
			for(String type : getTypesForIndex(index, typePattern)) {
				for(String fieldName : getFieldNamesByMapping(index, type)) {
					if (!fieldName.toLowerCase().matches(fieldPattern)) {
						continue;
					}
					matchedFields.add(fieldName);
				}
				result.getIndicesMappings().putAll(getMapping(index, type, matchedFields.toArray(new String [matchedFields.size()])));
				matchedFields.clear();
			}
		}
		return result;
	}

	public Map<String, Object> getMapping(String index, String typePattern, String... fields) {
		Map<String, Object> mappings = new HashMap<String, Object>();

		Map<String, Object> indexMapping = new HashMap<String, Object>();
		indexMapping.put("mappings", mappings);

		Map<String, Object> result = new HashMap<String, Object>();
		result.put(index, indexMapping);

		for (String type : getTypesForIndex(index, typePattern)) {
			final Map<String, Object> typeMappings = getIndexTypeMapping(index, type);
			final Map<String, Object> typeFieldMapping = new HashMap<String, Object>();
			
			for(String field : fields) {
				if (versionInfoService.getApiVersion().isNewerThan(ApiVersion.V_2_4_3)) {
					Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");

					Map<String, Object> mapping = new HashMap<String, Object>();
					if (properties.containsKey(field)) {
						mapping.put(field, properties.get(field));
					}

					Map<String, Object> fieldMapping = new HashMap<String, Object>();
					fieldMapping.put("full_name", field);
					fieldMapping.put("mapping", mapping);

					typeFieldMapping.put(field, fieldMapping);
				} else {
					typeFieldMapping.put(field, typeMappings.get(field));
				}
			}
			mappings.put(type, typeFieldMapping);
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

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws ElefanaException {
		final GetFieldNamesResponse result = new GetFieldNamesResponse();

		int totalIndices = 0;
		for (String index : indexUtils.listIndicesForIndexPattern(request.getIndexPattern())) {
			for(String type : getTypesForIndex(index, request.getTypePattern())) {
				result.getFieldNames().addAll(getFieldNames(index, type));
			}

			if(request.getMaxIndices() > 0 && totalIndices > request.getMaxIndices()) {
				break;
			}
			totalIndices++;
		}
		return result;
	}

	public GetFieldStatsResponse getFieldStats(String indexPattern) throws Exception {
		final GetFieldStatsResponse result = new GetFieldStatsResponse();
		
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);

		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			final Map<String, Object> indexResult = new HashMap<String, Object>();

			try {
				SqlRowSet rowSet = jdbcTemplate
						.queryForRowSet("SELECT _stats FROM elefana_index_field_stats WHERE _index = ? LIMIT 1", index);
				if (rowSet.next()) {
					indexResult.put("fields", JsonIterator.deserialize(rowSet.getString("_stats"), new TypeLiteral<Map<String, Object>>(){}));
					result.getIndices().put(index, indexResult);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public GetFieldCapabilitiesResponse getFieldCapabilities(String indexPattern) throws ElefanaException {
		final GetFieldCapabilitiesResponse result = new GetFieldCapabilitiesResponse();
		
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			try {
				SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
						"SELECT _capabilities FROM elefana_index_field_capabilities WHERE _index = ? LIMIT 1", index);
				if (rowSet.next()) {
					result.getFields().putAll(JsonIterator.deserialize(rowSet.getString("_capabilities"), new TypeLiteral<Map<String, Object>>(){}));
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		return result;
	}

	private Map<String, Object> getIndexTypeMappings(String index) {
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			SqlRowSet rowSet = jdbcTemplate
					.queryForRowSet("SELECT _type, _mapping FROM elefana_index_mapping WHERE _index = ?", index);
			while (rowSet.next()) {
				results.put(rowSet.getString("_type"), JsonIterator.deserialize(rowSet.getString("_mapping"), new TypeLiteral<Map<String, Object>>(){}));
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
				return JsonIterator.deserialize(rowSet.getString("_mapping"), new TypeLiteral<Map<String, Object>>(){});
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return new HashMap<String, Object>(fieldMapper.getEmptyMapping());
	}

	private void generateMappingsForQueuedTables() {
		try {
			while (!mappingQueue.isEmpty()) {
				QueuedIndex nextIndex = mappingQueue.peek();
				if(nextIndex.getTimestamp() > System.currentTimeMillis()) {
					lastMapping = System.currentTimeMillis();
					return;
				}
				nextIndex = mappingQueue.poll();

				Map<String, Object> mapping = getIndexTypeMappings(nextIndex.getIndex());

				if (mapping.isEmpty()) {
					IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(nextIndex.getIndex());

					SqlRowSet rowSet = getSampleDocuments(nextIndex.getIndex());
					int totalSamples = generateMappingsForAllTypes(indexTemplate, mapping, nextIndex.getIndex(), rowSet);
					if (totalSamples == 0) {
						rowSet = jdbcTemplate.queryForRowSet("SELECT _type, _source FROM " + IndexUtils.DATA_TABLE
								+ " WHERE _index = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(),
								nextIndex);
						generateMappingsForAllTypes(indexTemplate, mapping, nextIndex.getIndex(), rowSet);
					}
				} else {
					for (String type : mapping.keySet()) {
						final Set<String> existingFieldNames = getFieldNames(nextIndex.getIndex(), type);

						Map<String, Object> typeMappings = (Map) mapping.get(type);
						SqlRowSet rowSet = getSampleDocuments(nextIndex.getIndex(), type);
						int totalSamples = generateMappingsForType(typeMappings, existingFieldNames, nextIndex.getIndex(), type, rowSet);
						if (totalSamples == 0) {
							rowSet = jdbcTemplate.queryForRowSet("SELECT _source FROM " + IndexUtils.DATA_TABLE
									+ " WHERE _index = ? AND _type = ? LIMIT "
									+ nodeSettingsService.getFallbackMappingSampleSize(), nextIndex, type);
							generateMappingsForType(typeMappings, existingFieldNames, nextIndex.getIndex(), type, rowSet);
						}
					}
				}
				generateFieldCapabilitiesForIndex(nextIndex.getIndex());
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

			final Set<String> fieldNames = getFieldNames(index, type);

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
			Map<String, Object> document = JsonIterator.deserialize(rowSet.getString("_source"), new TypeLiteral<Map<String, Object>>(){});
			fieldMapper.generateMappings(typeMappings, document);
			fieldMapper.generateFieldNames(fieldNames, document);

			saveMappings(index, type, typeMappings);
			saveFieldNames(index, type, fieldNames);

			totalSamples++;
		}
		return totalSamples;
	}

	private int generateMappingsForType(Map<String, Object> typeMappings, Set<String> fieldNames,
	                                    String nextTable, String type,
			SqlRowSet rowSet) throws Exception {
		int totalSamples = 0;
		while (rowSet.next()) {
			Map<String, Object> document = JsonIterator.deserialize(rowSet.getString("_source"), new TypeLiteral<Map<String, Object>>(){});
			fieldMapper.generateMappings(typeMappings, document);
			fieldMapper.generateFieldNames(fieldNames, document);
			totalSamples++;
		}

		if (totalSamples == 0) {
			return 0;
		}

		saveMappings(nextTable, type, typeMappings);
		saveFieldNames(nextTable, type, fieldNames);
		return totalSamples;
	}

	private void generateFieldStatsForQueuedTables() {
		try {
			while (!fieldStatsQueue.isEmpty()) {
				QueuedIndex nextIndex = fieldStatsQueue.peek();
				if(nextIndex.getTimestamp() > System.currentTimeMillis()) {
					lastFieldStats = System.currentTimeMillis();
					return;
				}
				nextIndex = fieldStatsQueue.poll();

				generateFieldStatsForIndex(nextIndex.getIndex());
			}

			lastFieldStats = System.currentTimeMillis();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void generateFieldStatsForIndex(String indexName) throws Exception {
		final Map<String, V2FieldStats> fieldStats = new HashMap<String, V2FieldStats>();
		final List<String> types = getTypesForIndex(indexName);

		final String queryTarget = nodeSettingsService.isUsingCitus() ? indexUtils.getQueryTarget(indexName) : IndexUtils.DATA_TABLE;
		jdbcTemplate.execute("ANALYZE " + queryTarget);

		final String totalDocsQuery = nodeSettingsService.isUsingCitus()
				? "SELECT COUNT(_id) FROM " + queryTarget
				: "SELECT COUNT(_id) FROM " + queryTarget + " WHERE _index='" + indexName + "'";

		final long totalDocs = (long) jdbcTemplate.queryForList(totalDocsQuery).get(0).get("count");

		for (String type : types) {
			final List<String> fieldNames = getFieldNamesByMapping(indexName, type);
			indexUtils.ensureJsonFieldIndexExist(indexName, fieldNames);

			for (String fieldName : fieldNames) {
				if (fieldStats.containsKey(fieldName)) {
					continue;
				}
				final String totalDocsWithFieldQuery = nodeSettingsService.isUsingCitus()
						? "SELECT COUNT(*) FROM " + queryTarget + " WHERE _source ? '"
								+ fieldName + "'"
						: "SELECT COUNT(*) FROM " + queryTarget + " WHERE _index='" + indexName
								+ "' AND _source ? '" + fieldName + "'";

				final long totalDocsWithField = (long) jdbcTemplate.queryForList(totalDocsWithFieldQuery).get(0).get("count");

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

		final String json = JsonStream.serialize(fieldStats);
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(json);

		jdbcTemplate.update(
				"INSERT INTO elefana_index_field_stats (_index, _stats) VALUES (?, ?) ON CONFLICT (_index) DO UPDATE SET _stats = EXCLUDED._stats",
				indexName, jsonObject);
	}

	private void generateFieldCapabilitiesForIndex(String index) throws ElefanaException {
		generateFieldCapabilitiesForIndex(index, jdbcTemplate
				.queryForRowSet("SELECT _type, _mapping FROM elefana_index_mapping WHERE _index = ?", index));
	}

	private void generateFieldCapabilitiesForIndex(String index, SqlRowSet indexMappingSet) throws ElefanaException {
		if (!versionInfoService.getApiVersion().isNewerThan(ApiVersion.V_2_4_3)) {
			return;
		}

		Map<String, Object> fields = new HashMap<String, Object>();

		while (indexMappingSet.next()) {
			Map<String, Object> mappings = JsonIterator.deserialize(indexMappingSet.getString("_mapping"), new TypeLiteral<Map<String, Object>>(){});
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

		try {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(JsonStream.serialize(fields));
			jdbcTemplate.update(
					"INSERT INTO elefana_index_field_capabilities (_index, _capabilities) VALUES (?, ?) ON CONFLICT (_index) DO UPDATE SET _capabilities = EXCLUDED._capabilities",
					index, jsonObject);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
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
			return jdbcTemplate.queryForRowSet("SELECT _type, _source FROM " + indexUtils.getQueryTarget(index)
					+ " LIMIT " + nodeSettingsService.getFallbackMappingSampleSize());
		} else {
			return jdbcTemplate.queryForRowSet(
					"SELECT _type, _source FROM " + IndexUtils.DATA_TABLE + " TABLESAMPLE BERNOULLI("
							+ String.format("%f", nodeSettingsService.getMappingSampleSize()) + ") WHERE _index = ?",
					index);
		}
	}

	private SqlRowSet getSampleDocuments(String index, String type) {
		if (nodeSettingsService.isUsingCitus()) {
			return jdbcTemplate.queryForRowSet("SELECT _source FROM " + indexUtils.getQueryTarget(index)
					+ " WHERE _type = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(),
					type);
		} else {
			return jdbcTemplate.queryForRowSet("SELECT _source FROM " + IndexUtils.DATA_TABLE
					+ " TABLESAMPLE BERNOULLI(" + String.format("%f", nodeSettingsService.getMappingSampleSize())
					+ ") WHERE _index = ? AND _type = ?", index, type);
		}
	}

	public void scheduleIndexForStats(String index) {
		fieldStatsQueue.add(new QueuedIndex(index, System.currentTimeMillis() + nodeSettingsService.getFieldStatsInterval()));
	}

	public void scheduleIndexForMapping(String index) {
		mappingQueue.add(new QueuedIndex(index, System.currentTimeMillis() + nodeSettingsService.getMappingInterval()));
	}

	public void scheduleIndexForMappingAndStats(String index) {
		scheduleIndexForMapping(index);
		scheduleIndexForStats(index);
	}

	private void saveFieldNames(String index, String type, Set<String> fieldNames) throws ElefanaException {
		try {
			final String json = JsonStream.serialize(fieldNames);
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(json);

			jdbcTemplate.update(
					"INSERT INTO elefana_index_field_names (_tracking_id, _index, _type, _field_names) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _field_names = EXCLUDED._field_names",
					index + "-" + type, index, type, jsonObject);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
	}

	private void saveMappings(String index, String type, Map<String, Object> mappings) throws ElefanaException {
		try {
			final String json = JsonStream.serialize(mappings);
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(json);

			jdbcTemplate.update(
					"INSERT INTO elefana_index_mapping (_tracking_id, _index, _type, _mapping) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
					index + "-" + type, index, type, jsonObject);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
	}

	@Override
	public GetFieldNamesRequest prepareGetFieldNames(String indexPattern) {
		return new PsqlGetFieldNamesRequest(this, indexPattern);
	}

	@Override
	public GetFieldNamesRequest prepareGetFieldNames(String indexPattern, String typePattern) {
		return new PsqlGetFieldNamesRequest(this, indexPattern, typePattern);
	}

	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings() {
		return new PsqlGetFieldMappingsRequest(this);
	}

	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this);
		result.setIndicesPattern(indexPattern);
		return result;
	}

	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern, String typePattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this);
		result.setIndicesPattern(indexPattern);
		result.setTypesPattern(typePattern);
		return result;
	}
	
	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern, String typePattern, String fieldPattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this);
		result.setIndicesPattern(indexPattern);
		result.setTypesPattern(typePattern);
		result.setFieldPattern(fieldPattern);
		return result;
	}
	
	@Override
	public PutFieldMappingRequest preparePutFieldMappings(String index, String mappings) {
		PsqlPutFieldMappingRequest result = new PsqlPutFieldMappingRequest(this, index);
		result.setMappings(mappings);
		return result;
	}

	@Override
	public PutFieldMappingRequest preparePutFieldMappings(String index, String type, String mappings) {
		PsqlPutFieldMappingRequest result = new PsqlPutFieldMappingRequest(this, index);
		result.setType(type);
		result.setMappings(mappings);
		return result;
	}
	
	@Override
	public GetFieldCapabilitiesRequest prepareGetFieldCapabilities(String indexPattern) {
		return new PsqlGetFieldCapabilitiesRequest(this, indexPattern);
	}

	@Override
	public GetFieldStatsRequest prepareGetFieldStats(String indexPattern) {
		return new PsqlGetFieldStatsRequest(this, indexPattern);
	}
	
	@Override
	public RefreshIndexRequest prepareRefreshIndex(String index) {
		return new PsqlRefreshIndexRequest(this, index);
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
