/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.indices;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.viridiansoftware.elefana.util.FormatDateTimeFormatter;
import com.viridiansoftware.elefana.util.JodaUtils;

public abstract class FieldMapper {
	public static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = JodaUtils.forPattern(
            "strict_date_optional_time||epoch_millis", Locale.ROOT);
	
	public static final FormatDateTimeFormatter EPOCH_DATE_TIME_FORMATTER = JodaUtils.forPattern(
            "epoch_millis", Locale.ROOT);

	public abstract Map<String, Object> getEmptyMapping();
	
	public abstract List<String> getFieldNames(Map<String, Object> mappings);
	
	public abstract Map<String, Object> convertIndexTemplateToMappings(IndexTemplate indexTemplate, String type);
	
	public abstract void generateMappings(Map<String, Object> existingMapping, Map<String, Object> document);
	
	public abstract void mergeMapping(Map<String, Object> existingMappingForField, String fieldName, Map<String, Object> newMappingForField);
}
