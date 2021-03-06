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
package com.elefana.search.psql;

import com.elefana.api.search.MultiSearchRequest;
import com.elefana.api.search.MultiSearchResponse;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlMultiSearchRequest extends MultiSearchRequest implements Callable<MultiSearchResponse> {
	private final PsqlSearchService searchService;

	public PsqlMultiSearchRequest(PsqlSearchService searchService, ChannelHandlerContext context,
	                              PooledStringBuilder requestBody) {
		super(searchService, context, requestBody);
		this.searchService = searchService;
	}

	@Override
	protected Callable<MultiSearchResponse> internalExecute() {
		return this;
	}

	@Override
	public MultiSearchResponse call() throws Exception {
		if(fallbackIndex != null && fallbackType != null) {
			return searchService.multiSearch(context, fallbackIndex, fallbackType, requestBody);
		} else if(fallbackIndex != null) {
			return searchService.multiSearch(context, fallbackIndex, requestBody);
		} else {
			return searchService.multiSearch(context, requestBody);
		}
	}
}
