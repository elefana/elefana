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
import java.util.Map;

import com.elefana.api.indices.GetFieldMappingsResponse;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.Decoder;
import com.jsoniter.spi.TypeLiteral;

public class GetFieldMappingsResponseDecoder implements Decoder {

	@Override
	public Object decode(JsonIterator iter) throws IOException {
		GetFieldMappingsResponse result = new GetFieldMappingsResponse();
		result.getIndicesMappings().putAll(iter.read(new TypeLiteral<Map<String, Object>>(){}));
		return result;
	}

}
