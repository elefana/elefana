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

import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class GetIndexTemplateForIndexResponseDecoder extends StdDeserializer<GetIndexTemplateForIndexResponse> {

	public GetIndexTemplateForIndexResponseDecoder() {
		this(null);
	}

	public GetIndexTemplateForIndexResponseDecoder(Class<?> vc) {
		super(vc);
	}

	@Override
	public GetIndexTemplateForIndexResponse deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		final JsonNode any = parser.getCodec().readTree(parser);

		final String index = any.get("index").toString();
		final String tempalteId = any.get("templateId").isNull() ? null : any.get("templateId").textValue();
		final IndexTemplate indexTemplate = any.get("template").isNull() ? null : JsonUtils.OBJECT_MAPPER.readValue(any.get("template").toString(), IndexTemplate.class);

		final GetIndexTemplateForIndexResponse result = new GetIndexTemplateForIndexResponse(index, tempalteId);
		result.setIndexTemplate(indexTemplate);
		return result;
	}
}