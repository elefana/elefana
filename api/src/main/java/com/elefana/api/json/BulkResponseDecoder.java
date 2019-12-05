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

import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkOpType;
import com.elefana.api.document.BulkResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class BulkResponseDecoder extends StdDeserializer<BulkResponse> {

	public BulkResponseDecoder() {
		this(null);
	}

	protected BulkResponseDecoder(Class<?> vc) {
		super(vc);
	}

	@Override
	public BulkResponse deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode any = parser.getCodec().readTree(parser);
		BulkResponse result = new BulkResponse();
		result.setErrors(any.get("errors").asBoolean());
		result.setTook(any.get("took").asLong());

		JsonNode itemsAny = any.get("items");
		for(int i = 0; i < itemsAny.size(); i++) {
			JsonNode itemAny = itemsAny.get(i);

			JsonNode responseAny = null;

			BulkItemResponse itemResponse = null;
			if(itemAny.get("index").isObject()) {
				responseAny = itemAny.get("index");
				itemResponse = new BulkItemResponse(i, BulkOpType.INDEX);
			} else if(itemAny.get("delete").isObject()) {
				responseAny = itemAny.get("delete");
				itemResponse = new BulkItemResponse(i, BulkOpType.DELETE);
			}

			itemResponse.setIndex(responseAny.get("_index").asText());
			itemResponse.setType(responseAny.get("_type").asText());
			itemResponse.setId(responseAny.get("_id").asText());
			itemResponse.setResult(responseAny.get("result").asText());

			if(!itemResponse.isFailed()) {
				itemResponse.setVersion(responseAny.get("_version").asInt());
			}
			result.getItems().add(itemResponse);
		}

		return result;
	}
}
