/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

import com.elefana.api.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

public class IndexStorageSettingsTest {

	@Test
	public void testSerializationWithDefaultInstance() {
		final IndexStorageSettings storageSettings = new IndexStorageSettings();

		final String json = JsonUtils.toJsonString(storageSettings);
		System.out.println(json);
		Assert.assertEquals(storageSettings, JsonUtils.fromJsonString(json, IndexStorageSettings.class));
	}

	@Test
	public void testDeserializationWithEmptyInstance() {
		final IndexStorageSettings storageSettings = new IndexStorageSettings();

		final String json = "{}";
		Assert.assertEquals(storageSettings, JsonUtils.fromJsonString(json, IndexStorageSettings.class));
	}

	@Test
	public void testDeserializationWithSomeFields() {
		final String json ="{\"distribution\": \"TIME\", \"timestamp_path\": \"timestamp\", \"field_stats_disabled\": true}";
		final IndexStorageSettings result = JsonUtils.fromJsonString(json, IndexStorageSettings.class);
		Assert.assertEquals("timestamp", result.getTimestampPath());
		Assert.assertEquals(DistributionMode.TIME, result.getDistributionMode());
		Assert.assertEquals(true, result.isFieldStatsDisabled());
	}

	@Test
	public void testDeserializationWithInvalidFields() {
		final String json ="{\"distribution\": \"TIME\", \"timestamp_path\": \"timestamp\", \"disable_field_stats\": true}";
		final IndexStorageSettings result = JsonUtils.fromJsonString(json, IndexStorageSettings.class);
		Assert.assertEquals("timestamp", result.getTimestampPath());
		Assert.assertEquals(DistributionMode.TIME, result.getDistributionMode());
		Assert.assertEquals(false, result.isFieldStatsDisabled());
	}
}
