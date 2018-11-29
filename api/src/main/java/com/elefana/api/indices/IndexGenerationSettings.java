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

import java.util.ArrayList;
import java.util.List;

import com.jsoniter.annotation.JsonProperty;

public class IndexGenerationSettings {
	@JsonProperty("mode")
	private IndexGenerationMode mode = IndexGenerationMode.ALL;
	@JsonProperty("preset_fields")
	private List<String> presetFields;

	public IndexGenerationMode getMode() {
		if(mode == null) {
			mode = IndexGenerationMode.ALL;
		}
		return mode;
	}

	public void setMode(IndexGenerationMode mode) {
		this.mode = mode;
	}

	public List<String> getPresetFields() {
		if(presetFields == null) {
			presetFields = new ArrayList<String>();
		}
		return presetFields;
	}

	public void setPresetFields(List<String> presetFields) {
		this.presetFields = presetFields;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mode == null) ? 0 : mode.hashCode());
		result = prime * result + ((presetFields == null) ? 0 : presetFields.hashCode());
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
		IndexGenerationSettings other = (IndexGenerationSettings) obj;
		if (mode != other.mode)
			return false;
		if (presetFields == null) {
			if (other.presetFields != null)
				return false;
		} else if (!presetFields.equals(other.presetFields))
			return false;
		return true;
	}
}
