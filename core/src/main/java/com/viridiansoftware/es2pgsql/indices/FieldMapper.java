/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.indices;

import java.util.Locale;
import java.util.Map;

import com.viridiansoftware.es2pgsql.util.FormatDateTimeFormatter;
import com.viridiansoftware.es2pgsql.util.JodaUtils;

public abstract class FieldMapper {
	public static final FormatDateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = JodaUtils.forPattern(
            "strict_date_optional_time||epoch_millis", Locale.ROOT);

	public abstract Map<String, Object> getEmptyMapping();
	
	public abstract void generateMappings(Map<String, Object> existingMapping, Map<String, Object> document);
	
	public abstract void mergeMapping(Map<String, Object> existingMappingForField, String fieldName, Map<String, Object> newMappingForField);
}
