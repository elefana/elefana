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
import java.util.Objects;

import com.jsoniter.annotation.JsonProperty;

public class IndexGenerationSettings {
	@JsonProperty("mode")
	private IndexGenerationMode mode = IndexGenerationMode.ALL;
	@JsonProperty("preset_index_fields")
	private List<String> presetIndexFields;
	@JsonProperty("index_delay_seconds")
	private long indexDelaySeconds;

	public IndexGenerationMode getMode() {
		if(mode == null) {
			mode = IndexGenerationMode.ALL;
		}
		return mode;
	}

	public void setMode(IndexGenerationMode mode) {
		this.mode = mode;
	}

	public List<String> getPresetIndexFields() {
		if(presetIndexFields == null) {
			presetIndexFields = new ArrayList<String>();
		}
		return presetIndexFields;
	}

	public void setPresetIndexFields(List<String> presetIndexFields) {
		this.presetIndexFields = presetIndexFields;
	}

	public long getIndexDelaySeconds() {
		return indexDelaySeconds;
	}

	public void setIndexDelaySeconds(long indexDelaySeconds) {
		this.indexDelaySeconds = indexDelaySeconds;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof IndexGenerationSettings)) return false;
		IndexGenerationSettings that = (IndexGenerationSettings) o;
		return indexDelaySeconds == that.indexDelaySeconds &&
				mode == that.mode &&
				Objects.equals(presetIndexFields, that.presetIndexFields);
	}

	@Override
	public int hashCode() {
		return Objects.hash(mode, presetIndexFields, indexDelaySeconds);
	}
}
