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

import com.elefana.api.indices.IndexTemplate;
import org.joda.time.DateTime;

import java.util.*;

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
	public List<String> getFieldNamesFromMapping(Map<String, Object> mappings) {
		Map<String, Object> propertyMappings = (Map<String, Object>) mappings.get("properties");
		if(propertyMappings == null) {
			return new ArrayList<String>();
		}
		return new ArrayList<String>(propertyMappings.keySet());
	}
	
	@Override
	public Map<String, Object> getEmptyMapping() {
		return EMPTY_MAPPING;
	}
	
	@Override
	public Map<String, Object> convertIndexTemplateToMappings(IndexTemplate indexTemplate, String type) {
		final Map<String, Object> result = new HashMap<String, Object>();
		if(!indexTemplate.getMappings().containsKey(type)) {
			return result;
		}
		return (Map) indexTemplate.getMappings().get(type);
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
