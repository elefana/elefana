/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.joda.time.DateTime;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viridiansoftware.es2pgsql.node.NodeSettingsService;
import com.viridiansoftware.es2pgsql.util.FormatDateTimeFormatter;
import com.viridiansoftware.es2pgsql.util.JodaUtils;
import com.viridiansoftware.es2pgsql.util.TableUtils;

@Service
public class IndexFieldMappingService implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMappingService.class);
	private static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = JodaUtils.forPattern(
            "strict_date_optional_time||epoch_millis", Locale.ROOT);
	private static final Map<String, Object> EMPTY_MAPPING = new HashMap<String, Object>();

	private final SortedSet<String> mappingQueue = new ConcurrentSkipListSet<String>();

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private TaskScheduler taskScheduler;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private TableUtils tableUtils;

	private final ObjectMapper objectMapper = new ObjectMapper();

	@PostConstruct
	public void postConstruct() {
		Map<String, Object> allMapping = new HashMap<String, Object>();
		allMapping.put("enabled", false);
		
		EMPTY_MAPPING.put("_all", allMapping);
		EMPTY_MAPPING.put("properties", new HashMap<String, Object>());
		
		try {
			ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator(new ClassPathResource("/functions.sql", IndexFieldMappingService.class));
			resourceDatabasePopulator.setSeparator(ScriptUtils.EOF_STATEMENT_SEPARATOR);
			resourceDatabasePopulator.execute(jdbcTemplate.getDataSource());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_mapping (_tracking_id VARCHAR(255) PRIMARY KEY, _table_name VARCHAR(255), _type VARCHAR(255), _mapping jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_mapping_tracking (_table_name VARCHAR(255) PRIMARY KEY, _last_insert_time BIGINT, _last_mapping_time BIGINT);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_field_capabilities (_table_name VARCHAR(255) PRIMARY KEY, _capabilities jsonb);");
		if (nodeSettingsService.isUsingCitus()) {
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_mapping', '_tracking_id');");
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_mapping_tracking', '_table_name');");
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_field_capabilities', '_table_name');");
		}

		taskScheduler.scheduleWithFixedDelay(this, nodeSettingsService.getMappingInterval());
	}
	
	public List<String> getTypesForTableName(String tableName) {
		return jdbcTemplate.queryForList("SELECT _type FROM es2pgsql_index_mapping WHERE _table_name = ?", String.class, tableName);
	}
	
	public List<String> getTypesForTableName(String tableName, String typePattern) {
		String [] typePatterns = typePattern.split(",");
		
		List<String> results = new ArrayList<String>(1);
		List<String> sqlResults = getTypesForTableName(tableName);
	
		for(String type : sqlResults) {
			for(String pattern : typePatterns) {
				pattern = pattern.replace(".", "\\$");
				pattern = pattern.replace("*", "(.*)");
				pattern = "^" + pattern + "$";
				LOGGER.info(type + " " + pattern);
				
				if (type.matches(pattern)) {
					results.add(type);
					break;
				}
			}
		}
		return results;
	}
	
	public Map<String, Object> getIndexMappings() throws Exception {
		Map<String, Object> result = new HashMap<String, Object>();
		for(String index : tableUtils.listTables()) {
			result.put(index, result);
		}
		return result;
	}
	
	public Map<String, Object> getIndexMapping(String index) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("mappings", getIndexMappingsFromTable(TableUtils.sanitizeTableName(index)));
		return result;
	}

	public Map<String, Object> getIndexMapping(String index, String type) {
		Map<String, Object> mappings = new HashMap<String, Object>();
		mappings.put(type, getIndexTypeMappingFromTable(TableUtils.sanitizeTableName(index), type));
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("mappings", mappings);
		return result;
	}
	
	public void putIndexMapping(String index, String type, String mappingBody) throws Exception {
		String tableName = TableUtils.sanitizeTableName(index);
		Map<String, Object> existingMappings = getIndexTypeMappingFromTable(tableName, type);
		Map<String, Object> newMappings = objectMapper.readValue(mappingBody, Map.class);
		if(newMappings.containsKey(type)) {
			newMappings = (Map<String, Object>) newMappings.get(type);
		}
		if(newMappings.containsKey("properties")) {
			newMappings = (Map<String, Object>) newMappings.get("properties");
		}
		for(String propertyName : newMappings.keySet()) {
			existingMappings.put(propertyName, newMappings.get(propertyName));
		}
		saveMappings(tableName, type, existingMappings);
		generateFieldCapabilitiesForIndex(tableName);
	}
	
	public Map<String, Object> getIndexMapping(String index, String typePattern, String field) {		
		Map<String, Object> mappings = new HashMap<String, Object>();
		
		Map<String, Object> indexMapping = new HashMap<String, Object>();
		indexMapping.put("mappings", mappings);
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(index, indexMapping);
		
		for(String type : getTypesForTableName(TableUtils.sanitizeTableName(index), typePattern)) {
			Map<String, Object> typeMappings = getIndexTypeMappingFromTable(TableUtils.sanitizeTableName(index), type);
			Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");
			
			Map<String, Object> mapping = new HashMap<String, Object>();
			if(properties.containsKey(field)) {
				mapping.put(field, properties.get(field));
			}
			
			Map<String, Object> fieldMapping = new HashMap<String, Object>();
			fieldMapping.put("full_name", field);
			fieldMapping.put("mapping", mapping);
			
			Map<String, Object> typeFieldMapping = new HashMap<String, Object>();
			typeFieldMapping.put(field, fieldMapping);
			
			mappings.put(type, typeFieldMapping);
		}
		return result;
	}
	
	public String getFirstFieldMapping(List<String> indices, String [] typePatterns, String field) {
		for(String index : indices) {
			if(typePatterns.length == 0) {
				String result = getFirstFieldMapping(index, "*", field);
				if(result != null) {
					return result;
				}
			} else {
				for(String typePattern : typePatterns) {
					String result = getFirstFieldMapping(index, typePattern, field);
					if(result != null) {
						return result;
					}
				}
			}
		}
		return null;
	}
	
	public String getFirstFieldMapping(String index, String typePattern, String field) {
		for(String type : getTypesForTableName(TableUtils.sanitizeTableName(index), typePattern)) {
			Map<String, Object> typeMappings = getIndexTypeMappingFromTable(TableUtils.sanitizeTableName(index), type);
			Map<String, Object> properties = (Map<String, Object>) typeMappings.get("properties");
			if(properties.containsKey(field)) {
				Map<String, Object> property = (Map) properties.get(field);
				return (String) property.get("type");
			}
		}
		return null;
	}
	
	public Map<String, Object> getFieldCapabilities(String indexPattern) throws Exception {
		Map<String, Object> fields = new HashMap<String, Object>();
		for(String index : tableUtils.listTables(indexPattern)) {
			try {
				SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
						"SELECT _capabilities FROM es2pgsql_index_field_capabilities WHERE _table_name = ? LIMIT 1", TableUtils.sanitizeTableName(index));
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
	
	private Map<String, Object> getIndexMappingsFromTable(String tableName) {
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
					"SELECT _type, _mapping FROM es2pgsql_index_mapping WHERE _table_name = ?", tableName);
			while (rowSet.next()) {
				results.put(rowSet.getString("_type"), objectMapper.readValue(rowSet.getString("_mapping"), Map.class));
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return results;
	}

	private Map<String, Object> getIndexTypeMappingFromTable(String tableName, String type) {
		try {
			SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
					"SELECT _mapping FROM es2pgsql_index_mapping WHERE _table_name = ? AND _type = ? LIMIT 1", tableName, type);
			if (rowSet.next()) {
				return objectMapper.readValue(rowSet.getString("_mapping"), Map.class);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return new HashMap<String, Object>(EMPTY_MAPPING);
	}

	@Override
	public void run() {
		generateMappingsForQueuedTables();
		checkTablesForNewInserts();
	}

	private void generateMappingsForQueuedTables() {
		try {
			while (!mappingQueue.isEmpty()) {
				String nextTable = mappingQueue.first();

				Map<String, Object> mapping = getIndexMappingsFromTable(nextTable);

				if(mapping.isEmpty()) {
					SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
							"SELECT _type, _source FROM " + nextTable + " TABLESAMPLE BERNOULLI("
									+  String.format("%f", nodeSettingsService.getMappingSampleSize()) + ")");
					
					int totalSamples = generateMappingsForAllTypes(mapping, nextTable, rowSet);
					if(totalSamples == 0) {
						rowSet = jdbcTemplate.queryForRowSet(
								"SELECT _type, _source FROM " + nextTable + " LIMIT 100");
						generateMappingsForAllTypes(mapping, nextTable, rowSet);
					}
				} else {
					for(String type : mapping.keySet()) {
						Map<String, Object> typeMappings = (Map) mapping.get(type);
						SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
								"SELECT _source FROM " + nextTable + " TABLESAMPLE BERNOULLI("
										+ String.format("%f", nodeSettingsService.getMappingSampleSize()) + ") WHERE _type = ?",
								type);
						int totalSamples = generateMappingsForType(typeMappings, nextTable, type, rowSet);
						if(totalSamples == 0) {
							rowSet = jdbcTemplate.queryForRowSet(
									"SELECT _source FROM " + nextTable + " WHERE _type = ? LIMIT 100",
									type);
							generateMappingsForType(typeMappings, nextTable, type, rowSet);
						}
					}
				}
				generateFieldCapabilitiesForIndex(nextTable);
				
				mappingQueue.remove(nextTable);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	private int generateMappingsForAllTypes(Map<String, Object> mapping, String nextTable, SqlRowSet rowSet) throws Exception {
		int totalSamples = 0;
		while (rowSet.next()) {
			final String type = rowSet.getString("_type");
			
			Map<String, Object> typeMappings = (Map) mapping.get(type);
			if(typeMappings == null) {
				typeMappings = new HashMap<String, Object>();
				mapping.put(type, typeMappings);
			}
			Map<String, Object> document = objectMapper.readValue(rowSet.getString("_source"), Map.class);
			generateMappings(typeMappings, document);
			
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(objectMapper.writeValueAsString(typeMappings));

			jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping (_tracking_id, _table_name, _type, _mapping) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
					nextTable + "-" + type, nextTable, type, jsonObject);
			jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping_tracking (_table_name, _last_mapping_time) VALUES (?, ?) ON CONFLICT (_table_name) DO UPDATE SET _last_mapping_time = EXCLUDED._last_mapping_time",
					nextTable, System.currentTimeMillis());
			totalSamples++;
		}
		return totalSamples;
	}
	
	private int generateMappingsForType(Map<String, Object> typeMappings, String nextTable, String type, SqlRowSet rowSet) throws Exception {
		int totalSamples = 0;
		while (rowSet.next()) {
			Map<String, Object> document = objectMapper.readValue(rowSet.getString("_source"), Map.class);
			generateMappings(typeMappings, document);
			totalSamples++;
		}
		
		if(totalSamples == 0) {
			return 0;
		}
		
		saveMappings(nextTable, type, typeMappings);
		return totalSamples;
	}
	
	private void generateFieldCapabilitiesForIndex(String tableName) throws Exception {
		generateFieldCapabilitiesForIndex(tableName, jdbcTemplate.queryForRowSet(
				"SELECT _type, _mapping FROM es2pgsql_index_mapping WHERE _table_name = ?", tableName));
	}
	
	private void generateFieldCapabilitiesForIndex(String tableName, SqlRowSet indexMappingSet) throws Exception {
		Map<String, Object> fields = new HashMap<String, Object>();
		
		while(indexMappingSet.next()) {
			Map<String, Object> mappings = objectMapper.readValue(indexMappingSet.getString("_mapping"), Map.class);
			Map<String, Object> properties = (Map) mappings.get("properties");
			for(String propertyKey : properties.keySet()) {
				if(fields.containsKey(propertyKey)) {
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
		jdbcTemplate.update("INSERT INTO es2pgsql_index_field_capabilities (_table_name, _capabilities) VALUES (?, ?) ON CONFLICT (_table_name) DO UPDATE SET _capabilities = EXCLUDED._capabilities",
				tableName, jsonObject);
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
	
	private void checkTablesForNewInserts() {
		try {
			if(nodeSettingsService.isUsingCitus()) {
				//TODO: Use distributed trigger, see -> https://github.com/citusdata/citus/issues/906
				mappingQueue.addAll(tableUtils.listTables());
			} else {
				SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
						"SELECT _table_name FROM es2pgsql_index_mapping_tracking WHERE _last_insert_time > _last_mapping_time");
				while (rowSet.next()) {
					mappingQueue.add(rowSet.getString("_table_name"));
				}
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	private void generateMappings(Map<String, Object> mapping, Map<String, Object> document) {
		Map<String, Object> propertyMappings = (Map<String, Object>) mapping.get("properties");
		if(propertyMappings == null) {
			propertyMappings = new HashMap<String, Object>();
			mapping.put("properties", propertyMappings);
		}
		
		for(String propertyName : document.keySet()) {
			if(propertyMappings.containsKey(propertyName)) {
				continue;
			}
			
			if(document.get(propertyName) instanceof String) {
				String value = (String) document.get(propertyName);
				
				try {
					if(DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(value) > 0L) {
						propertyMappings.put(propertyName, generateMappingType("date"));
					}
					continue;
				} catch (Exception e) {}
				if(value.contains(" ")) {
					propertyMappings.put(propertyName, generateMappingType("text"));
				} else {
					propertyMappings.put(propertyName, generateMappingType("string"));
				}
			} else if(document.get(propertyName) instanceof Boolean) {
				propertyMappings.put(propertyName, generateMappingType("boolean"));
			} else if(document.get(propertyName) instanceof Byte) {
				propertyMappings.put(propertyName, generateMappingType("long"));
			} else if(document.get(propertyName) instanceof Short) {
				propertyMappings.put(propertyName, generateMappingType("long"));
			} else if(document.get(propertyName) instanceof Integer) {
				propertyMappings.put(propertyName, generateMappingType("long"));
			} else if(document.get(propertyName) instanceof Long) {
				propertyMappings.put(propertyName, generateMappingType("long"));
			} else if(document.get(propertyName) instanceof Double) {
				propertyMappings.put(propertyName, generateMappingType("double"));
			} else if(document.get(propertyName) instanceof Float) {
				propertyMappings.put(propertyName, generateMappingType("double"));
			} else if(document.get(propertyName) instanceof Date) {
				propertyMappings.put(propertyName, generateMappingType("date"));
			} else if(document.get(propertyName) instanceof DateTime) {
				propertyMappings.put(propertyName, generateMappingType("date"));
			} else if(document.get(propertyName) instanceof List) {
				propertyMappings.put(propertyName, generateMappingType("nested"));
			} else {
				propertyMappings.put(propertyName, generateMappingType("object"));
			}
		}
	}
	
	private Map<String, Object> generateMappingType(String type) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("type", type);
		return result;
	}
	
	private void saveMappings(String tableName, String type, Map<String, Object> mappings) throws Exception {
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(objectMapper.writeValueAsString(mappings));

		jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping (_tracking_id, _table_name, _type, _mapping) VALUES (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
				tableName + "-" + type, tableName, type, jsonObject);
		jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping_tracking (_table_name, _last_mapping_time) VALUES (?, ?) ON CONFLICT (_table_name) DO UPDATE SET _last_mapping_time = EXCLUDED._last_mapping_time",
				tableName, System.currentTimeMillis());
	}
}
