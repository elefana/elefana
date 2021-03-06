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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

public class IndexTemplate {
	private final Map<String, Object> settings = new HashMap<String, Object>();
	
	private String template;
	private IndexStorageSettings storage = new IndexStorageSettings();
	private Map<String, Object> mappings;
	
	public IndexTemplate() {
		super();
		settings.put("number_of_shards", 1);
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	@JsonIgnore
	public boolean isTimeSeries() {
		if(storage == null) {
			return false;
		}
		return storage.getDistributionMode().equals(DistributionMode.TIME);
	}
	
	@JsonIgnore
	public boolean isTimestampField(String fieldName) {
		if(storage == null) {
			return false;
		}
		if(storage.getTimestampPath() == null) {
			return false;
		}
		return storage.getTimestampPath().equalsIgnoreCase(fieldName);
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

	public IndexStorageSettings getStorage() {
		return storage;
	}

	public void setStorage(IndexStorageSettings storage) {
		this.storage = storage;
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
		result = prime * result + ((storage == null) ? 0 : storage.hashCode());
		result = prime * result + ((template == null) ? 0 : template.hashCode());
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
		if (storage == null) {
			if (other.storage != null)
				return false;
		} else if (!storage.equals(other.storage))
			return false;
		if (template == null) {
			if (other.template != null)
				return false;
		} else if (!template.equals(other.template))
			return false;
		return true;
	}
}
