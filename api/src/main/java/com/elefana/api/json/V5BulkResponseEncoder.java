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
import com.elefana.api.document.BulkResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class V5BulkResponseEncoder extends StdSerializer<BulkResponse> {

	public V5BulkResponseEncoder() {
		this(null);
	}

	public V5BulkResponseEncoder(Class<BulkResponse> clazz) {
		super(clazz);
	}

	@Override
	public void serialize(BulkResponse bulkResponse, JsonGenerator stream, SerializerProvider provider) throws IOException {
		stream.writeStartObject();

		stream.writeNumberField("took", bulkResponse.getTook());
		stream.writeBooleanField("errors", bulkResponse.isErrors());

		stream.writeFieldName("items");
		stream.writeStartArray();

		for(int i = 0; i < bulkResponse.getItems().size(); i++) {
			BulkItemResponse itemResponse = bulkResponse.getItems().get(i);
			stream.writeStartObject();

			switch(itemResponse.getOpType()) {
			case DELETE:
				stream.writeFieldName("delete");
				break;
			case INDEX:
			default:
				stream.writeFieldName("index");
				break;
			}

			stream.writeStartObject();
			stream.writeStringField("_index", itemResponse.getIndex());
			stream.writeStringField("_type", itemResponse.getType());
			stream.writeStringField("_id", itemResponse.getId());
			stream.writeStringField("result", itemResponse.getResult());

			if(!itemResponse.isFailed()) {
				stream.writeNumberField("_version", itemResponse.getVersion());
			}

			stream.writeEndObject();
			stream.writeEndObject();
		}

		stream.writeEndArray();

		stream.writeEndObject();
	}
}
