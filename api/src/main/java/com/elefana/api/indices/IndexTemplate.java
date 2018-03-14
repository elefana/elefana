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
package com.elefana.api.indices;

import java.util.HashMap;
import java.util.Map;

import com.jsoniter.annotation.JsonIgnore;

public class IndexTemplate {
	private final Map<String, Object> settings = new HashMap<String, Object>();
	
	private String template;
	private String timestamp_path;
	private Map<String, Object> mappings;
	
	@JsonIgnore
	private String templateId;
	
	public IndexTemplate() {
		super();
		settings.put("number_of_shards", 1);
	}
	
	public IndexTemplate(String templateId) {
		this();
		this.templateId = templateId;
	}
	
	public String getTemplateId() {
		return templateId;
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	public String getTimestamp_path() {
		return timestamp_path;
	}

	public void setTimestamp_path(String timestamp_path) {
		this.timestamp_path = timestamp_path;
	}

	public boolean isTimeSeries() {
		return timestamp_path != null;
	}

	public Map<String, Object> getSettings() {
		return settings;
	}

	public Map<String, Object> getMappings() {
		return mappings;
	}

	public void setMappings(Map<String, Object> mappings) {
		this.mappings = mappings;
	}

	@Override
	public String toString() {
		return "IndexTemplate [settings=" + settings + ", template=" + template + ", mappings=" + mappings + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mappings == null) ? 0 : mappings.hashCode());
		result = prime * result + ((settings == null) ? 0 : settings.hashCode());
		result = prime * result + ((template == null) ? 0 : template.hashCode());
		result = prime * result + ((templateId == null) ? 0 : templateId.hashCode());
		result = prime * result + ((timestamp_path == null) ? 0 : timestamp_path.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexTemplate other = (IndexTemplate) obj;
		if (mappings == null) {
			if (other.mappings != null)
				return false;
		} else if (!mappings.equals(other.mappings))
			return false;
		if (settings == null) {
			if (other.settings != null)
				return false;
		} else if (!settings.equals(other.settings))
			return false;
		if (template == null) {
			if (other.template != null)
				return false;
		} else if (!template.equals(other.template))
			return false;
		if (templateId == null) {
			if (other.templateId != null)
				return false;
		} else if (!templateId.equals(other.templateId))
			return false;
		if (timestamp_path == null) {
			if (other.timestamp_path != null)
				return false;
		} else if (!timestamp_path.equals(other.timestamp_path))
			return false;
		return true;
	}
}
