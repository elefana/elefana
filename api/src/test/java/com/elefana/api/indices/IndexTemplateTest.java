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

import java.util.HashMap;

public class IndexTemplateTest {

	@Test
	public void testSerializationWithDefaultInstance() {
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setTemplate("template1");

		final String json = JsonUtils.toJsonString(indexTemplate);
		System.out.println(json);
		Assert.assertEquals(indexTemplate, JsonUtils.fromJsonString(json, IndexTemplate.class));
	}

	@Test
	public void testSerializationWithMappings() {
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setTemplate("template2");
		indexTemplate.setMappings(new HashMap<String, Object>());
		indexTemplate.getMappings().put("key1", "value1");
		indexTemplate.getMappings().put("key2", new HashMap<String, Object>());

		final String json = JsonUtils.toJsonString(indexTemplate);
		Assert.assertEquals(indexTemplate, JsonUtils.fromJsonString(json, IndexTemplate.class));
	}

	@Test
	public void testSerializationWithDefaultStorageSettings() {
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setTemplate("template3");
		indexTemplate.getStorage().getIndexGenerationSettings().getPresetBrinIndexFields().add("field");

		final String json = JsonUtils.toJsonString(indexTemplate);
		Assert.assertEquals(indexTemplate, JsonUtils.fromJsonString(json, IndexTemplate.class));
	}

	@Test
	public void testSerializationWithBucketStorageSettings() {
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setTemplate("template3");
		indexTemplate.getStorage().setIndexTimeBucket(IndexTimeBucket.DAY);
		indexTemplate.getStorage().setTimestampPath("timestamp");

		final String json = JsonUtils.toJsonString(indexTemplate);
		Assert.assertEquals(indexTemplate, JsonUtils.fromJsonString(json, IndexTemplate.class));
	}

	@Test
	public void testDeserializationWithNoStorageSettings() {
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setTemplate("template1");
		final String json = "{\"settings\":{\"number_of_shards\":1},\"template\":\"template1\",\"mappings\":null}";
		Assert.assertEquals(indexTemplate, JsonUtils.fromJsonString(json, IndexTemplate.class));
	}
}
