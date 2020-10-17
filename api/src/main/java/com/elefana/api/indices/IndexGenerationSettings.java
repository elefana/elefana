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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IndexGenerationSettings {
	@JsonProperty("mode")
	private IndexGenerationMode mode = IndexGenerationMode.NONE;
	@JsonProperty("preset_hash_index_fields")
	private List<String> presetHashIndexFields = new ArrayList<String>(1);
	@JsonProperty("preset_brin_index_fields")
	private List<String> presetBrinIndexFields = new ArrayList<String>(1);
	@JsonProperty("preset_gin_index_fields")
	private List<String> presetGinIndexFields = new ArrayList<String>(1);
	@JsonProperty("index_delay_seconds")
	private long indexDelaySeconds;

	public IndexGenerationMode getMode() {
		if(mode == null) {
			mode = IndexGenerationMode.NONE;
		}
		return mode;
	}

	public void setMode(IndexGenerationMode mode) {
		this.mode = mode;
	}

	public List<String> getPresetHashIndexFields() {
		return presetHashIndexFields;
	}

	public void setPresetHashIndexFields(List<String> presetHashIndexFields) {
		this.presetHashIndexFields = presetHashIndexFields;
	}

	public List<String> getPresetBrinIndexFields() {
		return presetBrinIndexFields;
	}

	public void setPresetBrinIndexFields(List<String> presetBrinIndexFields) {
		this.presetBrinIndexFields = presetBrinIndexFields;
	}

	public List<String> getPresetGinIndexFields() {
		return presetGinIndexFields;
	}

	public void setPresetGinIndexFields(List<String> presetGinIndexFields) {
		this.presetGinIndexFields = presetGinIndexFields;
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
				Objects.equals(presetHashIndexFields, that.presetHashIndexFields) &&
				Objects.equals(presetBrinIndexFields, that.presetBrinIndexFields) &&
				Objects.equals(presetGinIndexFields, that.presetGinIndexFields);
	}

	@Override
	public int hashCode() {
		return Objects.hash(mode, presetHashIndexFields, presetBrinIndexFields, presetGinIndexFields, indexDelaySeconds);
	}

	@Override
	public String toString() {
		return "IndexGenerationSettings{" +
				"mode=" + mode +
				", presetHashIndexFields=" + presetHashIndexFields +
				", presetBrinIndexFields=" + presetBrinIndexFields +
				", presetGinIndexFields=" + presetGinIndexFields +
				", indexDelaySeconds=" + indexDelaySeconds +
				'}';
	}
}
