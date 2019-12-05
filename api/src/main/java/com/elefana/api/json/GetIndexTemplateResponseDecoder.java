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
package com.elefana.api.json;

import com.elefana.api.indices.GetIndexTemplateResponse;
import com.elefana.api.indices.IndexTemplate;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Iterator;

public class GetIndexTemplateResponseDecoder extends StdDeserializer<GetIndexTemplateResponse> {

	public GetIndexTemplateResponseDecoder() {
		this(null);
	}

	public GetIndexTemplateResponseDecoder(Class<?> vc) {
		super(vc);
	}

	@Override
	public GetIndexTemplateResponse deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		final JsonNode any = parser.getCodec().readTree(parser);

		final GetIndexTemplateResponse result = new GetIndexTemplateResponse();
		final Iterator<String> templateNames = any.fieldNames();
		while(templateNames.hasNext()) {
			final String templateId = templateNames.next();
			result.getTemplates().put(templateId, JsonUtils.OBJECT_MAPPER.readValue(any.get(templateId).toString(), IndexTemplate.class));
		}
		return result;
	}
}
