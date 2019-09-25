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
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.Decoder;

import java.io.IOException;

public class NodeStatsDecoder implements Decoder {

	@Override
	public Object decode(JsonIterator iter) throws IOException {
		Any any = iter.readAny();

		NodeStats result = null;
		if (any.get("http_address").valueType().equals(ValueType.STRING)
				|| any.get("http_address").valueType().equals(ValueType.NULL)) {
			result = any.as(V2NodeStats.class);
		} else {
			result = any.as(V5NodeStats.class);
		}
		return result;
	}

}
