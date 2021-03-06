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
package com.elefana.api.search;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

public abstract class MultiSearchRequest extends ApiRequest<MultiSearchResponse> {
	protected String fallbackIndex, fallbackType;
	protected PooledStringBuilder requestBody;

	public MultiSearchRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                          PooledStringBuilder requestBody) {
		super(requestExecutor, context);
		this.requestBody = requestBody;
	}

	public String getFallbackIndex() {
		return fallbackIndex;
	}

	public void setFallbackIndex(String fallbackIndex) {
		this.fallbackIndex = fallbackIndex;
	}

	public String getFallbackType() {
		return fallbackType;
	}

	public void setFallbackType(String fallbackType) {
		this.fallbackType = fallbackType;
	}

	public PooledStringBuilder getRequestBody() {
		return requestBody;
	}

	public void setRequestBody(PooledStringBuilder requestBody) {
		this.requestBody = requestBody;
	}

}
