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
package com.elefana.api.indices;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

public abstract class PutFieldMappingRequest extends ApiRequest<PutFieldMappingResponse> {
	protected String index;
	protected String type;
	protected PooledStringBuilder mappings;
	
	public PutFieldMappingRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                              String index) {
		super(requestExecutor, context);
		this.index = index;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		if(index == null) {
			return;
		}
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		if(type == null) {
			return;
		}
		this.type = type;
	}

	public PooledStringBuilder getMappings() {
		return mappings;
	}

	public void setMappings(PooledStringBuilder mappings) {
		if(mappings == null) {
			return;
		}
		this.mappings = mappings;
	}
}
