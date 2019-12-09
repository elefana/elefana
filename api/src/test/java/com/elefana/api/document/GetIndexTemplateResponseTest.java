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
package com.elefana.api.document;

import com.elefana.api.indices.*;
import com.elefana.api.json.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class GetIndexTemplateResponseTest {

	@Test
	public void testEncodeDecode() {
		final String index = "logs-01-01-1970";
		final String templateId = "template1";

		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setMappings(new HashMap<String, Object>());

		final GetIndexTemplateResponse expected = new GetIndexTemplateResponse(templateId);
		expected.setIndexTemplate(indexTemplate);

		final String json = expected.toJsonString();
		System.out.println(json);
		final GetIndexTemplateResponse result = JsonUtils.fromJsonString(json, GetIndexTemplateResponse.class);
		Assert.assertEquals(expected.toJsonString(), result.toJsonString());
		Assert.assertEquals(expected.getIndexTemplate(), result.getIndexTemplate());
		Assert.assertEquals(expected.getIndexTemplate().getStorage(), result.getIndexTemplate().getStorage());
	}

	@Test
	public void testDecodeWithNoStorageSettings() {
		final String json = "{\"templateA\":{\"settings\":{\"number_of_shards\":1},\"template\":\"f25aa8b9-2df6-456d-949e-b17558d6554d\",\"storage\":{\"distribution\":\"HASH\",\"time_bucket\":\"MINUTE\",\"timestamp_path\":null,\"index_generation\":{\"mode\":\"ALL\",\"preset_index_fields\":[],\"index_delay_seconds\":0},\"field_stats_disabled\":false,\"mapping_disabled\":false,\"brin_enabled\":false,\"gin_enabled\":false},\"mappings\":{\"test\":{\"_source\":{\"enabled\":false},\"properties\":{\"nonDocField\":{\"type\":\"date\"}}}}}}";
		final GetIndexTemplateResponse result = JsonUtils.fromJsonString(json, GetIndexTemplateResponse.class);
		result.setTemplateId("templateA");
		Assert.assertEquals("f25aa8b9-2df6-456d-949e-b17558d6554d", result.getIndexTemplate().getTemplate());
		Assert.assertNull(result.getIndexTemplate().getStorage().getTimestampPath());
	}
}
