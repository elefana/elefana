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

import com.elefana.api.node.NodeStats;
import com.elefana.api.node.v2.V2NodeStats;
import com.elefana.api.node.v5.V5NodeStats;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class NodeStatsDecoder extends JsonDeserializer<NodeStats> {

	@Override
	public NodeStats deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		final JsonNode any = ctxt.readValue(parser, JsonNode.class);

		NodeStats result = null;
		if (any.has("http_address") && (any.get("http_address").isTextual() || any.get("http_address").isNull())) {
			result = JsonUtils.OBJECT_MAPPER.readValue(any.toString(), V2NodeStats.class);
		} else {
			result = JsonUtils.OBJECT_MAPPER.readValue(any.toString(), V5NodeStats.class);
		}
		return result;
	}
}
