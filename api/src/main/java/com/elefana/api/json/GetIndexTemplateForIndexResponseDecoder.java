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
package com.elefana.api.json;

import java.io.IOException;

import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.Decoder;

public class GetIndexTemplateForIndexResponseDecoder implements Decoder {

	@Override
	public Object decode(JsonIterator iter) throws IOException {
		Any any = iter.readAny();

		final String index = any.get("index").toString();
		final String tempalteId = any.get("templateId").valueType().equals(ValueType.NULL) ? null
				: any.get("templateId").toString();
		final IndexTemplate indexTemplate = any.get("template").valueType().equals(ValueType.NULL) ? null
				: any.get("template").as(IndexTemplate.class);

		GetIndexTemplateForIndexResponse result = new GetIndexTemplateForIndexResponse(index, tempalteId);
		result.setIndexTemplate(indexTemplate);
		return result;
	}

}