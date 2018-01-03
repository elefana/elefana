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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.elefana.es.compat.date.FormatDateTimeFormatter;
import com.elefana.es.compat.date.JodaUtils;

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
