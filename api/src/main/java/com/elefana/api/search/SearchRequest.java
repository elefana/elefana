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

public abstract class SearchRequest extends ApiRequest<SearchResponse> {
	private static final PooledStringBuilder DEFAULT_QUERY = PooledStringBuilder.allocate("{\"query\":{\"match_all\": {}}}");
	
	protected String indexPattern, typePattern;
	protected PooledStringBuilder requestBody;
	
	public SearchRequest(RequestExecutor requestExecutor, ChannelHandlerContext context) {
		this(requestExecutor, context, DEFAULT_QUERY);
	}

	public SearchRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                     PooledStringBuilder requestBody) {
		super(requestExecutor, context);
		this.requestBody = requestBody;
	}

	public String getIndexPattern() {
		return indexPattern;
	}

	public void setIndexPattern(String indexPattern) {
		this.indexPattern = indexPattern;
	}

	public String getTypePattern() {
		return typePattern;
	}

	public void setTypePattern(String typePattern) {
		this.typePattern = typePattern;
	}

	public PooledStringBuilder getRequestBody() {
		return requestBody;
	}

	public void setRequestBody(PooledStringBuilder requestBody) {
		this.requestBody = requestBody;
	}

}
