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
import com.elefana.api.document.BulkOpType;
import com.elefana.api.document.BulkResponse;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.Decoder;

public class BulkResponseDecoder implements Decoder {

	@Override
	public Object decode(JsonIterator iter) throws IOException {
		Any any = iter.readAny();
		BulkResponse result = new BulkResponse();
		result.setErrors(any.get("errors").toBoolean());
		result.setTook(any.get("took").toLong());
		
		Any itemsAny = any.get("items");
		for(int i = 0; i < itemsAny.size(); i++) {
			Any itemAny = itemsAny.get(i);
		
			Any responseAny = null;
			
			BulkItemResponse itemResponse = null;
			if(itemAny.get("index").valueType().equals(ValueType.OBJECT)) {
				responseAny = itemAny.get("index");
				itemResponse = new BulkItemResponse(i, BulkOpType.INDEX);
			} else if(itemAny.get("delete").valueType().equals(ValueType.OBJECT)) {
				responseAny = itemAny.get("delete");
				itemResponse = new BulkItemResponse(i, BulkOpType.DELETE);
			}
			
			itemResponse.setIndex(responseAny.get("_index").toString());
			itemResponse.setType(responseAny.get("_type").toString());
			itemResponse.setId(responseAny.get("_id").toString());
			itemResponse.setResult(responseAny.get("result").toString());
			
			if(!itemResponse.isFailed()) {
				itemResponse.setVersion(responseAny.get("_version").toInt());
			}
			result.getItems().add(itemResponse);
		}
		
		return result;
	}

}
