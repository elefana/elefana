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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.elefana.api.indices.IndexTemplate;

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
	public List<String> getFieldNames(Map<String, Object> mappings) {
		return new ArrayList<String>(mappings.keySet());
	}
	
	@Override
	public Map<String, Object> convertIndexTemplateToMappings(IndexTemplate indexTemplate, String type) {
		final Map<String, Object> result = new HashMap<String, Object>();
		if(!indexTemplate.getMappings().containsKey(type)) {
			return result;
		}
		
		final Map<String, Object> typeMappings = (Map) indexTemplate.getMappings().get(type);
		if(!typeMappings.containsKey("properties")) {
			return result;
		}
		final Map<String, Object> propertyMappings = (Map) typeMappings.get("properties");
		for(String propertyName : propertyMappings.keySet()) {
			Map<String, Object> propertyValues = (Map) propertyMappings.get(propertyName);
			String fieldType = (String) propertyValues.get("type");
			String format = (String) propertyValues.get("format");
			
			if(fieldType == null) {
				continue;
			}
			result.put(propertyName, generateMappingType(propertyName, fieldType, format));
		}
		return result;
	}

	@Override
	public void generateMappings(Map<String, Object> existingMapping, Map<String, Object> document) {
		final Map<String, Object> newMappings = new HashMap<String, Object>();
		
		for(String propertyName : document.keySet()) {
			if(document.get(propertyName) instanceof String) {
				String value = (String) document.get(propertyName);
				
				try {
					if(DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(value) > 0L) {
						try {
							if(EPOCH_DATE_TIME_FORMATTER.parser().parseMillis(value) > 0L) {
								newMappings.put(propertyName, generateMappingType(propertyName, "date", "epoch_millis"));
							} else {
								newMappings.put(propertyName, generateMappingType(propertyName, "date", "strict_date_optional_time"));
							}
						} catch (Exception e) {
							newMappings.put(propertyName, generateMappingType(propertyName, "date", "strict_date_optional_time"));
						}
					}
					continue;
				} catch (Exception e) {}
				if(value.contains(" ")) {
					newMappings.put(propertyName, generateMappingType(propertyName, "text"));
				} else {
					newMappings.put(propertyName, generateMappingType(propertyName, "string"));
				}
			} else if(document.get(propertyName) instanceof Boolean) {
				newMappings.put(propertyName, generateMappingType(propertyName, "boolean"));
			} else if(document.get(propertyName) instanceof Byte) {
				newMappings.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Short) {
				newMappings.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Integer) {
				newMappings.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Long) {
				newMappings.put(propertyName, generateMappingType(propertyName, "long"));
			} else if(document.get(propertyName) instanceof Double) {
				newMappings.put(propertyName, generateMappingType(propertyName, "double"));
			} else if(document.get(propertyName) instanceof Float) {
				newMappings.put(propertyName, generateMappingType(propertyName, "double"));
			} else if(document.get(propertyName) instanceof Date) {
				newMappings.put(propertyName, generateMappingType(propertyName, "date"));
			} else if(document.get(propertyName) instanceof DateTime) {
				newMappings.put(propertyName, generateMappingType(propertyName, "date"));
			} else if(document.get(propertyName) instanceof List) {
				newMappings.put(propertyName, generateMappingType(propertyName, "nested"));
			} else {
				newMappings.put(propertyName, generateMappingType(propertyName, "object"));
			}
		}
		
		for(String propertyName : document.keySet()) {
			if(!existingMapping.containsKey(propertyName)) {
				existingMapping.put(propertyName, newMappings.get(propertyName));
			}
		}
	}

	@Override
	public void mergeMapping(Map<String, Object> existingMapping, String fieldName, Map<String, Object> newMapping) {
		if(!existingMapping.containsKey("mapping")) {
			existingMapping.put("mapping", new HashMap<String, Object>());
			existingMapping.put("full_name", fieldName);
		}
		final Map<String, Object> mapping = (Map<String, Object>) existingMapping.get("mapping");
		
		if(!mapping.containsKey(fieldName)) {
			mapping.put(fieldName, generateMappingData(null));
		}
		final Map<String, Object> fieldMapping = (Map<String, Object>) mapping.get(fieldName);
		
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
		replaceIfPresent(fieldMapping, newMapping, "format");
		replaceIfPresent(fieldMapping, newMapping, "numeric_resolution");
	}
	
	private void replaceIfPresent(Map<String, Object> existingMapping, Map<String, Object> newMapping, String propertyName) {
		if(!newMapping.containsKey(propertyName)) {
			return;
		}
		existingMapping.put(propertyName, newMapping.get(propertyName));
	}
	
	private Map<String, Object> generateMappingType(String fieldName, String type) {
		return generateMappingType(fieldName, type, null);
	}
	
	private Map<String, Object> generateMappingType(String fieldName, String type, String format) {
		Map<String, Object> mapping = new HashMap<String, Object>();
		mapping.put(fieldName, generateMappingData(type, format));
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("full_name", fieldName);
		result.put("mapping", mapping);
		return result;
	}
	
	private Map<String, Object> generateMappingData(String type) {
		return generateMappingData(type, null);
	}
	
	private Map<String, Object> generateMappingData(String type, String format) {
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
		
		if(format != null) {
			mappingData.put("format", format);
		}
		return mappingData;
	}
}
