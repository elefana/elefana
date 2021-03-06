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
import com.elefana.api.json.JsonUtils;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.FieldMapper;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.V2FieldMapper;
import com.elefana.indices.V5FieldMapper;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.HashDiskBackedQueue;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.channel.ChannelHandlerContext;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
@DependsOn("nodeSettingsService")
public class PsqlIndexFieldMappingService implements IndexFieldMappingService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMappingService.class);

	private static final String MAPPING_QUEUE_ID = "index-field-mapping-queue";
	private static final int MAPPING_EXPECTED_ENTRIES = 10_000;
	private static final String MAPPING_AVERAGE_KEY = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
	private static final QueuedIndex MAPPING_AVERAGE_VALUE = new QueuedIndex(
			MAPPING_AVERAGE_KEY, System.currentTimeMillis());

	private final AtomicLong lastMapping = new AtomicLong(-1L);

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

	private HashDiskBackedQueue<QueuedIndex> mappingQueue;

	private ExecutorService executorService;
	private ScheduledFuture<?> mappingScheduledTask;

	private FieldMapper fieldMapper;

	private Cache<String, Set<String>> fieldNamesCache;
	private Cache<String, List<String>> typesByIndexCache;

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

		fieldNamesCache = CacheBuilder.newBuilder().
				expireAfterWrite(Duration.ofHours(environment.getProperty("elefana.service.field.cache.names.expire.hours", Integer.class, 1))).
				maximumSize(environment.getProperty("elefana.service.field.cache.names.expire.size", Integer.class, 250)).
				build();
		typesByIndexCache = CacheBuilder.newBuilder().
				expireAfterWrite(Duration.ofHours(environment.getProperty("elefana.service.field.cache.types.expire.hours", Integer.class, 1))).
				maximumSize(environment.getProperty("elefana.service.field.cache.types.expire.size", Integer.class, 250)).
				build();

		mappingQueue = new HashDiskBackedQueue(MAPPING_QUEUE_ID,
				nodeSettingsService.getDataDirectory(), QueuedIndex.class,
				MAPPING_EXPECTED_ENTRIES,
				MAPPING_AVERAGE_KEY, MAPPING_AVERAGE_VALUE);

		final int totalThreads = environment.getProperty("elefana.service.field.threads", Integer.class, 2);
		executorService = Executors.newFixedThreadPool(totalThreads, new NamedThreadFactory(
				"elefana-fieldMappingService-requestExecutor", ThreadPriorities.FIELD_MAPPING_SERVICE));

		mappingScheduledTask = taskScheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					generateMappingsForQueuedTables();
					mappingQueue.prune();
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}, nodeSettingsService.getMappingInterval());
	}

	@PreDestroy
	public void preDestroy() {
		if (mappingScheduledTask != null) {
			mappingScheduledTask.cancel(false);
		}
		executorService.shutdown();

		try {
			executorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}

		mappingQueue.dispose();
	}

	public List<String> getFieldNamesByMapping(String index, String type) {
		return fieldMapper.getFieldNamesFromMapping(getIndexTypeMapping(index, type));
	}

	public Set<String> getFieldNames(final String index, final String type) {
		try {
			final String key = index + "|" + type;
			final Set<String> result = fieldNamesCache.get(key, new Callable<Set<String>>() {
				@Override
				public Set<String> call() throws Exception {
					return getFieldNamesFromDatabase(index, type);
				}
			});
			if(result.isEmpty()) {
				fieldNamesCache.invalidate(key);
			}
			return result;
 		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getFieldNamesFromDatabase(index, type);
	}

	private Set<String> getFieldNamesFromDatabase(String index, String type) {
		final Set<String> results = new HashSet<String>();
		try {
			SqlRowSet rowSet = jdbcTemplate
					.queryForRowSet("SELECT _field_names FROM elefana_index_field_names WHERE _index = ? AND _type = ?", index, type);
			while (rowSet.next()) {
				results.addAll(JsonUtils.fromJsonString(rowSet.getString("_field_names"), List.class));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}

	public List<String> getTypesForIndex(final String index) {
		try {
			final List<String> result = typesByIndexCache.get(index, new Callable<List<String>>() {
				@Override
				public List<String> call() throws Exception {
					return getTypesForIndexFromDatabase(index);
				}
			});
			if(result.size() == 0) {
				typesByIndexCache.invalidate(index);
			}
			return result;
		} catch (ExecutionException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return getTypesForIndexFromDatabase(index);
	}

	private List<String> getTypesForIndexFromDatabase(String index) {
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

	public void putMapping(String index, PooledStringBuilder mappingBody) throws ElefanaException {
		Map<String, Object> mappings = (Map<String, Object>) JsonUtils.fromJsonString(mappingBody.toString(), Map.class).get("mappings");
		if (mappings == null) {
			return;
		}
		for (String type : mappings.keySet()) {
			putIndexMapping(index, type, (Map<String, Object>) mappings.get(type));
		}
	}

	public void putMapping(String index, String type, PooledStringBuilder mappingBody) throws ElefanaException {
		putIndexMapping(index, type, JsonUtils.fromJsonString(mappingBody.toString(), Map.class));
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

	public GetFieldCapabilitiesResponse getFieldCapabilities(String indexPattern) throws ElefanaException {
		final GetFieldCapabilitiesResponse result = new GetFieldCapabilitiesResponse();
		
		for (String index : indexUtils.listIndicesForIndexPattern(indexPattern)) {
			try {
				SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
						"SELECT _capabilities FROM elefana_index_field_capabilities WHERE _index = ? LIMIT 1", index);
				if (rowSet.next()) {
					result.getFields().putAll(JsonUtils.fromJsonString(rowSet.getString("_capabilities"), Map.class));
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
				results.put(rowSet.getString("_type"), JsonUtils.fromJsonString(rowSet.getString("_mapping"), Map.class));
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
				return JsonUtils.fromJsonString(rowSet.getString("_mapping"), Map.class);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return new HashMap<String, Object>(fieldMapper.getEmptyMapping());
	}

	private void generateMappingsForQueuedTables() {
		try {
			final QueuedIndex nextIndex = new QueuedIndex();
			while (mappingQueue.peek(nextIndex)) {
				if(nextIndex.getTimestamp() > System.currentTimeMillis()) {
					lastMapping.set(System.currentTimeMillis());
					return;
				}
				if(!mappingQueue.poll(nextIndex)) {
					lastMapping.set(System.currentTimeMillis());
					return;
				}

				final Map<String, Object> mapping = getIndexTypeMappings(nextIndex.getIndex());

				final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(nextIndex.getIndex());
				if(indexTemplate != null && indexTemplate.getStorage() != null && !indexTemplate.getStorage().isMappingEnabled()) {
					generateEmptyMappingsForTypes(indexTemplate, mapping, nextIndex.getIndex());
					continue;
				}

				if (mapping.isEmpty()) {
					SqlRowSet rowSet = getSampleDocuments(nextIndex.getIndex());
					int totalSamples = generateMappingsForAllTypes(indexTemplate, mapping, nextIndex.getIndex(), rowSet);
					if (totalSamples == 0) {
						rowSet = jdbcTemplate.queryForRowSet("SELECT _type, _source FROM " + IndexUtils.DATA_TABLE
								+ " WHERE _index = ? LIMIT " + nodeSettingsService.getFallbackMappingSampleSize(),
								nextIndex.getIndex());
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
									+ nodeSettingsService.getFallbackMappingSampleSize(), nextIndex.getIndex(), type);
							generateMappingsForType(typeMappings, existingFieldNames, nextIndex.getIndex(), type, rowSet);
						}
					}
				}
				generateFieldCapabilitiesForIndex(nextIndex.getIndex());
			}
			lastMapping.set(System.currentTimeMillis());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void generateEmptyMappingsForTypes(IndexTemplate indexTemplate, Map<String, Object> mapping, String index) throws Exception {
		if(mapping.size() != 0) {
			return;
		}
		SqlRowSet rowSet = getSampleDocuments(index);
		while (rowSet.next()) {
			final String type = rowSet.getString("_type");

			Map<String, Object> typeMappings = (Map) mapping.get(type);
			if (typeMappings != null) {
				continue;
			}
			if (indexTemplate != null) {
				typeMappings = fieldMapper.convertIndexTemplateToMappings(indexTemplate, type);
			}
			if (typeMappings == null) {
				typeMappings = new HashMap<String, Object>();
			}
			mapping.put(type, typeMappings);
			saveMappings(index, type, typeMappings);
		}
		typesByIndexCache.invalidate(index);
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
			Map<String, Object> document = JsonUtils.fromJsonString(rowSet.getString("_source"), Map.class);
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
			Map<String, Object> document = JsonUtils.fromJsonString(rowSet.getString("_source"), Map.class);
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
			Map<String, Object> mappings = JsonUtils.fromJsonString(indexMappingSet.getString("_mapping"), Map.class);
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
			jsonObject.setValue(JsonUtils.toJsonString(fields));
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

	public void scheduleIndexForMapping(String index) {
		mappingQueue.offer(new QueuedIndex(index, System.currentTimeMillis() + nodeSettingsService.getMappingInterval()));
	}

	public void scheduleIndexForMappingAndStats(String index) {
		scheduleIndexForMapping(index);
	}

	private void saveFieldNames(String index, String type, Set<String> fieldNames) throws ElefanaException {
		try {
			final String json = JsonUtils.toJsonString(fieldNames);
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(json);

			jdbcTemplate.update(
					"INSERT INTO elefana_index_field_names (_tracking_id, _index, _type, _field_names) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _field_names = EXCLUDED._field_names",
					index + "-" + type, index, type, jsonObject);

			final String key = index + "|" + type;
			fieldNamesCache.invalidate(key);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
	}

	private void saveMappings(String index, String type, Map<String, Object> mappings) throws ElefanaException {
		try {
			final String json = JsonUtils.toJsonString(mappings);
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
	public GetFieldMappingsRequest prepareGetFieldMappings(ChannelHandlerContext context) {
		return new PsqlGetFieldMappingsRequest(this, context);
	}

	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(ChannelHandlerContext context, String indexPattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this, context);
		result.setIndicesPattern(indexPattern);
		return result;
	}

	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(ChannelHandlerContext context, String indexPattern, String typePattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this, context);
		result.setIndicesPattern(indexPattern);
		result.setTypesPattern(typePattern);
		return result;
	}
	
	@Override
	public GetFieldMappingsRequest prepareGetFieldMappings(ChannelHandlerContext context, String indexPattern, String typePattern, String fieldPattern) {
		PsqlGetFieldMappingsRequest result = new PsqlGetFieldMappingsRequest(this, context);
		result.setIndicesPattern(indexPattern);
		result.setTypesPattern(typePattern);
		result.setFieldPattern(fieldPattern);
		return result;
	}
	
	@Override
	public PutFieldMappingRequest preparePutFieldMappings(ChannelHandlerContext context, String index, PooledStringBuilder mappings) {
		PsqlPutFieldMappingRequest result = new PsqlPutFieldMappingRequest(this, context, index);
		result.setMappings(mappings);
		return result;
	}

	@Override
	public PutFieldMappingRequest preparePutFieldMappings(ChannelHandlerContext context, String index, String type, PooledStringBuilder mappings) {
		PsqlPutFieldMappingRequest result = new PsqlPutFieldMappingRequest(this, context, index);
		result.setType(type);
		result.setMappings(mappings);
		return result;
	}
	
	@Override
	public GetFieldCapabilitiesRequest prepareGetFieldCapabilities(ChannelHandlerContext context, String indexPattern) {
		return new PsqlGetFieldCapabilitiesRequest(this, context, indexPattern);
	}

	@Override
	public RefreshIndexRequest prepareRefreshIndex(ChannelHandlerContext context, String index) {
		return new PsqlRefreshIndexRequest(this, context, index);
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
