/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.indices;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

public class V5FieldMapper extends FieldMapper {
	private static final Map<String, Object> EMPTY_MAPPING = new HashMap<String, Object>();

	public V5FieldMapper() {
		super();
		Map<String, Object> allMapping = new HashMap<String, Object>();
		allMapping.put("enabled", false);
		
		EMPTY_MAPPING.put("_all", allMapping);
		EMPTY_MAPPING.put("properties", new HashMap<String, Object>());
	}
	
	@Override
	public Map<String, Object> getEmptyMapping() {
		return EMPTY_MAPPING;
	}
	
	@Override
	public void generateMappings(Map<String, Object> existingMapping, Map<String, Object> document) {
		Map<String, Object> propertyMappings = (Map<String, Object>) existingMapping.get("properties");
		if(propertyMappings == null) {
			propertyMappings = new HashMap<String, Object>();
			existingMapping.put("properties", propertyMappings);
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

	@Override
	public void mergeMapping(Map<String, Object> existingMapping, String fieldName, Map<String, Object> newMapping) {
		if(!existingMapping.containsKey("properties")) {
			existingMapping.put("properties", new HashMap<String, Object>());
		}
		existingMapping = (Map<String, Object>) existingMapping.get("properties");
		replaceIfPresent(existingMapping, newMapping, fieldName);
	}
	
	private void replaceIfPresent(Map<String, Object> existingMapping, Map<String, Object> newMapping, String propertyName) {
		if(!newMapping.containsKey(propertyName)) {
			return;
		}
		existingMapping.put(propertyName, newMapping.get(propertyName));
	}
}
