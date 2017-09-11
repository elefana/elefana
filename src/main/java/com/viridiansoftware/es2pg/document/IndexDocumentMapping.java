/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.document;

import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import com.viridiansoftware.es2pg.node.NodeSettingsService;
import com.viridiansoftware.es2pg.util.TableUtils;

@Service
public class IndexDocumentMapping implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(IndexDocumentMapping.class);
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
			ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator(new ClassPathResource("/functions.sql", IndexDocumentMapping.class));
			resourceDatabasePopulator.setSeparator(ScriptUtils.EOF_STATEMENT_SEPARATOR);
			resourceDatabasePopulator.execute(jdbcTemplate.getDataSource());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_mapping (_tracking_id VARCHAR(255) PRIMARY KEY, _table_name VARCHAR(255), _type VARCHAR(255), _mapping jsonb);");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS es2pgsql_index_mapping_tracking (_table_name VARCHAR(255) PRIMARY KEY, _last_insert_time BIGINT, _last_mapping_time BIGINT);");
		if (nodeSettingsService.isUsingCitus()) {
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_mapping', '_tracking_id');");
			jdbcTemplate.execute("SELECT create_distributed_table('es2pgsql_index_mapping_tracking', '_table_name');");
		}

		taskScheduler.scheduleWithFixedDelay(this, nodeSettingsService.getMappingInterval());
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
									+ nodeSettingsService.getMappingSampleSize() + ")");
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

						jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping (_tracking_id, _table_name, _type, _mapping) VALUE (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
								nextTable + "-" + type, nextTable, type, jsonObject);
						jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping_tracking (_table_name, _last_mapping_time) VALUE (?, ?) ON CONFLICT (_table_name) DO UPDATE SET _last_mapping_time = EXCLUDED._last_mapping_time",
								nextTable, System.currentTimeMillis());
					}
				} else {
					for(String type : mapping.keySet()) {
						Map<String, Object> typeMappings = (Map) mapping.get(type);
						SqlRowSet rowSet = jdbcTemplate.queryForRowSet(
								"SELECT _source FROM " + nextTable + " TABLESAMPLE BERNOULLI("
										+ nodeSettingsService.getMappingSampleSize() + ") WHERE _type = ?",
								type);
						while (rowSet.next()) {
							Map<String, Object> document = objectMapper.readValue(rowSet.getString("_source"), Map.class);
							generateMappings(typeMappings, document);
						}
						
						PGobject jsonObject = new PGobject();
						jsonObject.setType("json");
						jsonObject.setValue(objectMapper.writeValueAsString(typeMappings));

						jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping (_tracking_id, _table_name, _type, _mapping) VALUE (?, ?, ?, ?) ON CONFLICT (_tracking_id) DO UPDATE SET _mapping = EXCLUDED._mapping",
								nextTable + "-" + type, nextTable, type, jsonObject);
						jdbcTemplate.update("INSERT INTO es2pgsql_index_mapping_tracking (_table_name, _last_mapping_time) VALUE (?, ?) ON CONFLICT (_table_name) DO UPDATE SET _last_mapping_time = EXCLUDED._last_mapping_time",
								nextTable, System.currentTimeMillis());
					}
				}
				
				mappingQueue.remove(nextTable);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
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
		
		for(String propertyName : document.keySet()) {
			if(propertyMappings.containsKey(propertyName)) {
				continue;
			}
			
			if(document.get(propertyName) instanceof String) {
				propertyMappings.put(propertyName, generateMappingType("text"));
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
}
