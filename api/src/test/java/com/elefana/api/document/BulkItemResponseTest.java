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

import com.elefana.api.json.JsonUtils;
import com.elefana.api.json.V2BulkResponseEncoder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Test;


public class BulkItemResponseTest {

	@Test
	public void testEncodeDecode() {
		final SimpleModule module = new SimpleModule();
		module.addSerializer(BulkResponse.class, new V2BulkResponseEncoder());
		JsonUtils.OBJECT_MAPPER.registerModule(module);

		BulkResponse expected = new BulkResponse();
		expected.setErrors(true);
		expected.setTook(1001L);
		
		BulkItemResponse successResponse = new BulkItemResponse(0, BulkOpType.INDEX);
		successResponse.setIndex("index");
		successResponse.setType("type");
		successResponse.setId("123");
		successResponse.setResult(BulkItemResponse.STATUS_CREATED);
		successResponse.setVersion(1);
		expected.getItems().add(successResponse);
		
		BulkItemResponse failedResponse = new BulkItemResponse(1, BulkOpType.DELETE);
		failedResponse.setIndex("index");
		failedResponse.setType("type");
		failedResponse.setId("124");
		failedResponse.setResult(BulkItemResponse.STATUS_FAILED);
		expected.getItems().add(failedResponse);
		
		String json = JsonUtils.toJsonString(expected);
		BulkResponse result = JsonUtils.fromJsonString(json, BulkResponse.class);
		Assert.assertEquals(expected, result);
	}
}
