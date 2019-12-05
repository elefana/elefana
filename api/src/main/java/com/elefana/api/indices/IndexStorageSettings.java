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
	private DistributionMode distributionMode;
	@JsonProperty("time_bucket")
	private IndexTimeBucket indexTimeBucket;
	@JsonProperty("timestamp_path")
	private String timestampPath;
	@JsonProperty("index_generation")
	private IndexGenerationSettings indexGenerationSettings;
	@JsonProperty("field_stats_disabled")
	private boolean fieldStatsDisabled = false;
	@JsonProperty("mapping_disabled")
	private boolean mappingDisabled = false;
	@JsonProperty("brin_enabled")
	private boolean brinEnabled = false;
	@JsonProperty("gin_enabled")
	private boolean ginEnabled = false;

	public DistributionMode getDistributionMode() {
		if(distributionMode == null) {
			distributionMode = DistributionMode.HASH;
		}
		return distributionMode;
	}

	public void setDistributionMode(DistributionMode distributionMode) {
		this.distributionMode = distributionMode;
	}

	public IndexTimeBucket getIndexTimeBucket() {
		if(indexTimeBucket == null) {
			indexTimeBucket = IndexTimeBucket.MINUTE;
		}
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
		if(indexGenerationSettings == null) {
			indexGenerationSettings = new IndexGenerationSettings();
		}
		return indexGenerationSettings;
	}

	public void setIndexGenerationSettings(IndexGenerationSettings indexGenerationSettings) {
		this.indexGenerationSettings = indexGenerationSettings;
	}

	public boolean isFieldStatsDisabled() {
		return fieldStatsDisabled;
	}

	public void setFieldStatsDisabled(boolean fieldStatsDisabled) {
		this.fieldStatsDisabled = fieldStatsDisabled;
	}

	public boolean isMappingDisabled() {
		return mappingDisabled;
	}

	public void setMappingDisabled(boolean mappingDisabled) {
		this.mappingDisabled = mappingDisabled;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexStorageSettings that = (IndexStorageSettings) o;
		return fieldStatsDisabled == that.fieldStatsDisabled &&
				mappingDisabled == that.mappingDisabled &&
				brinEnabled == that.brinEnabled &&
				ginEnabled == that.ginEnabled &&
				distributionMode == that.distributionMode &&
				indexTimeBucket == that.indexTimeBucket &&
				Objects.equals(timestampPath, that.timestampPath) &&
				Objects.equals(indexGenerationSettings, that.indexGenerationSettings);
	}

	@Override
	public int hashCode() {
		return Objects.hash(distributionMode, indexTimeBucket, timestampPath, indexGenerationSettings,
				fieldStatsDisabled, mappingDisabled, brinEnabled, ginEnabled);
	}
}
