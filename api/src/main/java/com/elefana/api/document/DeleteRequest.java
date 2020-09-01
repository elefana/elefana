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
package com.elefana.api.document;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;
import io.netty.channel.ChannelHandlerContext;

public abstract class DeleteRequest extends ApiRequest<DeleteResponse> {
	protected final String index, type, id;

	public DeleteRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                     String index, String type, String id) {
		super(requestExecutor, context);
		this.index = index;
		this.type = type;
		this.id = id;
	}

	public String getIndex() {
		return index;
	}

	public String getType() {
		return type;
	}

	public String getId() {
		return id;
	}
}
