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

import com.elefana.api.indices.GetFieldMappingsResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Map;

public class GetFieldMappingsResponseDecoder extends StdDeserializer<GetFieldMappingsResponse> {

	public GetFieldMappingsResponseDecoder() {
		this(null);
	}

	public GetFieldMappingsResponseDecoder(Class<?> vc) {
		super(vc);
	}

	@Override
	public GetFieldMappingsResponse deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		GetFieldMappingsResponse result = new GetFieldMappingsResponse();
		result.getIndicesMappings().putAll(JsonUtils.fromJsonString(p.getText(), Map.class));
		return result;
	}
}
