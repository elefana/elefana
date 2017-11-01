/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.indices;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

public class V2FieldMapper extends FieldMapper {
	private static final Map<String, Object> EMPTY_MAPPING = new HashMap<String, Object>();
	
	public V2FieldMapper() {
		super();
	}
	
	@Override
	public Map<String, Object> getEmptyMapping() {
		return new HashMap<String, Object>(EMPTY_MAPPING);
	}

	@Override
	public void generateMappings(Map<String, Object> existingMapping, Map<String, Object> document) {
		for(String propertyName : document.keySet()) {
			if(existingMapping.containsKey(propertyName)) {
				continue;
			}
			
			if(document.get(propertyName) instanceof String) {
				String value = (String) document.get(propertyName);
				
				try {
					if(DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(value) > 0L) {
						existingMapping.put(propertyName, generateMappingType(propertyName, "date"));
					}
					continue;
				} catch (Exception e) {}
				if(value.contains(" ")) {
					existingMapping.put(propertyName, generateMappingType(propertyName, "text"));
				} else {
					existingMapping.put(propertyName, generateMappingType(propertyName, "string"));
				}
			} else if(document.get(propertyName) instanceof Boolean) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "boolean"));
			} else if(document.get(propertyName) instanceof Byte) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Short) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Integer) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Long) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Double) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "double"));
			} else if(document.get(propertyName) instanceof Float) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "double"));
			} else if(document.get(propertyName) instanceof Date) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "date"));
			} else if(document.get(propertyName) instanceof DateTime) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "date"));
			} else if(document.get(propertyName) instanceof List) {
				existingMapping.put(propertyName, generateMappingType(propertyName, "nested"));
			} else {
				existingMapping.put(propertyName, generateMappingType(propertyName, "object"));
			}
		}
	}
	
	private Map<String, Object> generateMappingType(String fieldName, String type) {
		Map<String, Object> mapping = new HashMap<String, Object>();
		mapping.put(fieldName, generateMappingData(type));
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("full_name", fieldName);
		result.put("mapping", mapping);
		return result;
	}

	@Override
	public void mergeMapping(Map<String, Object> existingMapping, String fieldName, Map<String, Object> newMapping) {
		if(!existingMapping.containsKey("mapping")) {
			existingMapping.put("mapping", new HashMap<String, Object>());
			existingMapping.put("full_name", fieldName);
		}
		Map<String, Object> mapping = (Map<String, Object>) existingMapping.get("mapping");
		
		if(!mapping.containsKey(fieldName)) {
			mapping.put(fieldName, generateMappingData(null));
		}
		Map<String, Object> fieldMapping = (Map<String, Object>) mapping.get(fieldName);
		
		replaceIfPresent(fieldMapping, newMapping, "type");
		replaceIfPresent(fieldMapping, newMapping, "boost");
		replaceIfPresent(fieldMapping, newMapping, "index");
		replaceIfPresent(fieldMapping, newMapping, "store");
		replaceIfPresent(fieldMapping, newMapping, "doc_values");
		replaceIfPresent(fieldMapping, newMapping, "term_vector");
		replaceIfPresent(fieldMapping, newMapping, "index_options");
		replaceIfPresent(fieldMapping, newMapping, "similarity");
		replaceIfPresent(fieldMapping, newMapping, "analyzer");
		replaceIfPresent(fieldMapping, newMapping, "null_value");
		replaceIfPresent(fieldMapping, newMapping, "include_in_all");
		replaceIfPresent(fieldMapping, newMapping, "position_increment_gap");
		replaceIfPresent(fieldMapping, newMapping, "ignore_above");
		replaceIfPresent(fieldMapping, newMapping, "norms");
		replaceIfPresent(fieldMapping, newMapping, "fielddata");
		replaceIfPresent(fieldMapping, newMapping, "fields");
	}
	
	private void replaceIfPresent(Map<String, Object> existingMapping, Map<String, Object> newMapping, String propertyName) {
		if(!newMapping.containsKey(propertyName)) {
			return;
		}
		existingMapping.put(propertyName, newMapping.get(propertyName));
	}
	
	private Map<String, Object> generateMappingData(String type) {
		Map<String, Object> norms = new HashMap<String, Object>();
		norms.put("enabled", true);
		
		Map<String, Object> mappingData = new HashMap<String, Object>();
		mappingData.put("type", type);
		mappingData.put("boost", 1);
		mappingData.put("index", "analyzed");
		mappingData.put("store", false);
		mappingData.put("doc_values", false);
		mappingData.put("term_vector", "no");
		mappingData.put("index_options", "positions");
		mappingData.put("similarity", "default");
		mappingData.put("analyzer", "default");
		mappingData.put("null_value", null);
		mappingData.put("include_in_all", false);
		mappingData.put("position_increment_gap", -1);
		mappingData.put("ignore_above", -1);
		mappingData.put("norms", norms);
		mappingData.put("fielddata", new HashMap<String, Object>());
		return mappingData;
	}
}
