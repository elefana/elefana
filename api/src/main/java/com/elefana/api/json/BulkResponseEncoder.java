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

import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkResponse;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;

public class BulkResponseEncoder implements Encoder {

	@Override
	public void encode(Object obj, JsonStream stream) throws IOException {
		stream.writeObjectStart();
		
		BulkResponse bulkResponse = (BulkResponse) obj;
		stream.writeObjectField("took");
		stream.writeVal(bulkResponse.getTook());
		stream.writeMore();
		
		stream.writeObjectField("errors");
		stream.writeVal(bulkResponse.isErrors());
		stream.writeMore();
		
		stream.writeObjectField("items");
		stream.writeArrayStart();
		
		for(int i = 0; i < bulkResponse.getItems().size(); i++) {
			BulkItemResponse itemResponse = bulkResponse.getItems().get(i);
			stream.writeObjectStart();
			
			switch(itemResponse.getOpType()) {
			case DELETE:
				stream.writeObjectField("delete");
				break;
			case INDEX:
			default:
				stream.writeObjectField("index");
				break;
			}
			
			stream.writeObjectStart();
			stream.writeObjectField("_index");
			stream.writeVal(itemResponse.getIndex());
			stream.writeMore();
			
			stream.writeObjectField("_type");
			stream.writeVal(itemResponse.getType());
			stream.writeMore();
			
			stream.writeObjectField("_id");
			stream.writeVal(itemResponse.getId());
			stream.writeMore();
			
			stream.writeObjectField("result");
			stream.writeVal(itemResponse.getResult());
			
			if(!itemResponse.isFailed()) {
				stream.writeMore();
				stream.writeObjectField("_version");
				stream.writeVal(itemResponse.getVersion());
			}
			
			stream.writeObjectEnd();
			stream.writeObjectEnd();
			
			if(i < bulkResponse.getItems().size() - 1) {
				stream.writeMore();
			}
		}
		
		stream.writeArrayEnd();
		
		stream.writeObjectEnd();
	}

}
