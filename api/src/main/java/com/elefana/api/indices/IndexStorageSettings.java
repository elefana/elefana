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

import java.util.Objects;

public class IndexStorageSettings {
	@JsonProperty("distribution")
	private DistributionMode distributionMode = DistributionMode.HASH;
	@JsonProperty("time_bucket")
	private IndexTimeBucket indexTimeBucket = IndexTimeBucket.MINUTE;
	@JsonProperty("timestamp_path")
	private String timestampPath = null;
	@JsonProperty("index_generation")
	private IndexGenerationSettings indexGenerationSettings = new IndexGenerationSettings();
	@JsonProperty("field_stats_enabled")
	private boolean fieldStatsEnabled = true;
	@JsonProperty("mapping_enabled")
	private boolean mappingEnabled = true;
	@JsonProperty("brin_enabled")
	private boolean brinEnabled = false;
	@JsonProperty("gin_enabled")
	private boolean ginEnabled = false;
	@JsonProperty("hash_enabled")
	private boolean hashEnabled = false;
	@JsonProperty("id_enabled")
	private boolean idEnabled = true;

	public DistributionMode getDistributionMode() {
		return distributionMode;
	}

	public void setDistributionMode(DistributionMode distributionMode) {
		this.distributionMode = distributionMode;
	}

	public IndexTimeBucket getIndexTimeBucket() {
		return indexTimeBucket;
	}

	public void setIndexTimeBucket(IndexTimeBucket indexTimeBucket) {
		this.indexTimeBucket = indexTimeBucket;
	}

	public String getTimestampPath() {
		return timestampPath;
	}

	public void setTimestampPath(String timestampPath) {
		this.timestampPath = timestampPath;
	}

	public IndexGenerationSettings getIndexGenerationSettings() {
		return indexGenerationSettings;
	}

	public void setIndexGenerationSettings(IndexGenerationSettings indexGenerationSettings) {
		this.indexGenerationSettings = indexGenerationSettings;
	}

	public boolean isFieldStatsEnabled() {
		return fieldStatsEnabled;
	}

	public void setFieldStatsEnabled(boolean fieldStatsEnabled) {
		this.fieldStatsEnabled = fieldStatsEnabled;
	}

	public boolean isMappingEnabled() {
		return mappingEnabled;
	}

	public void setMappingEnabled(boolean mappingEnabled) {
		this.mappingEnabled = mappingEnabled;
	}

	public boolean isBrinEnabled() {
		return brinEnabled;
	}

	public void setBrinEnabled(boolean brinEnabled) {
		this.brinEnabled = brinEnabled;
	}

	public boolean isGinEnabled() {
		return ginEnabled;
	}

	public void setGinEnabled(boolean ginEnabled) {
		this.ginEnabled = ginEnabled;
	}

	public boolean isHashEnabled() {
		return hashEnabled;
	}

	public void setHashEnabled(boolean hashEnabled) {
		this.hashEnabled = hashEnabled;
	}

	public boolean isIdEnabled() {
		return idEnabled;
	}

	public void setIdEnabled(boolean idEnabled) {
		this.idEnabled = idEnabled;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexStorageSettings that = (IndexStorageSettings) o;
		return fieldStatsEnabled == that.fieldStatsEnabled &&
				mappingEnabled == that.mappingEnabled &&
				brinEnabled == that.brinEnabled &&
				ginEnabled == that.ginEnabled &&
				hashEnabled == that.hashEnabled &&
				idEnabled == that.idEnabled &&
				distributionMode == that.distributionMode &&
				indexTimeBucket == that.indexTimeBucket &&
				Objects.equals(timestampPath, that.timestampPath) &&
				Objects.equals(indexGenerationSettings, that.indexGenerationSettings);
	}

	@Override
	public int hashCode() {
		return Objects.hash(distributionMode, indexTimeBucket, timestampPath, indexGenerationSettings,
				fieldStatsEnabled, mappingEnabled, brinEnabled, ginEnabled, hashEnabled, idEnabled);
	}

	@Override
	public String toString() {
		return "IndexStorageSettings{" +
				"distributionMode=" + distributionMode +
				", indexTimeBucket=" + indexTimeBucket +
				", timestampPath='" + timestampPath + '\'' +
				", indexGenerationSettings=" + indexGenerationSettings +
				", fieldStatsDisabled=" + fieldStatsEnabled +
				", mappingDisabled=" + mappingEnabled +
				", brinEnabled=" + brinEnabled +
				", ginEnabled=" + ginEnabled +
				", hashEnabled=" + hashEnabled +
				'}';
	}
}
