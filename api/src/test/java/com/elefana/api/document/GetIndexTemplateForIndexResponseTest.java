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
package com.elefana.api.document;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;
import com.jsoniter.JsonIterator;


public class GetIndexTemplateForIndexResponseTest {

	@Test
	public void testEncodeDecode() {
		final String index = "logs-01-01-1970";
		final String templateId = "template1";
		
		final IndexTemplate indexTemplate = new IndexTemplate();
		indexTemplate.setMappings(new HashMap<String, Object>());
		
		final GetIndexTemplateForIndexResponse expected = new GetIndexTemplateForIndexResponse(index, templateId);
		expected.setIndexTemplate(indexTemplate);
		
		final String json = expected.toJsonString();
		final GetIndexTemplateForIndexResponse result = JsonIterator.deserialize(json, GetIndexTemplateForIndexResponse.class);
		Assert.assertEquals(expected.toJsonString(), result.toJsonString());
	}
	
	@Test
	public void testEncodeDecodeNoIndexTemplate() {
		final String index = "logs-01-01-1970";
		
		final GetIndexTemplateForIndexResponse expected = new GetIndexTemplateForIndexResponse(index, null);
		
		final String json = expected.toJsonString();
		final GetIndexTemplateForIndexResponse result = JsonIterator.deserialize(json, GetIndexTemplateForIndexResponse.class);
		Assert.assertEquals(expected, result);
	}
}
