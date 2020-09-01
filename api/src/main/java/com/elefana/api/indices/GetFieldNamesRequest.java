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
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.channel.ChannelHandlerContext;

public abstract class GetFieldNamesRequest extends ApiRequest<GetFieldNamesResponse> {
	@JsonIgnore
	protected final String indexPattern;
	@JsonIgnore
	protected final String typePattern;

	protected int maxIndices = 10;

	public GetFieldNamesRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                            String indexPattern) {
		this(requestExecutor, context, indexPattern, "*");
	}

	public GetFieldNamesRequest(RequestExecutor requestExecutor, ChannelHandlerContext context,
	                            String indexPattern, String typePattern) {
		super(requestExecutor, context,false);
		this.indexPattern = indexPattern;
		this.typePattern = typePattern;
	}

	public String getIndexPattern() {
		return indexPattern;
	}

	public String getTypePattern() {
		return typePattern;
	}

	public int getMaxIndices() {
		return maxIndices;
	}

	public void setMaxIndices(int maxIndices) {
		this.maxIndices = maxIndices;
	}
}
