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

import com.elefana.api.search.SearchRequest;
import com.elefana.api.search.SearchResponse;

import java.util.concurrent.Callable;

public class PsqlSearchRequest extends SearchRequest implements Callable<SearchResponse> {
	private final PsqlSearchService searchService;

	public PsqlSearchRequest(PsqlSearchService searchService, String requestBody) {
		super(searchService, requestBody);
		this.searchService = searchService;
	}

	@Override
	protected Callable<SearchResponse> internalExecute() {
		return this;
	}

	@Override
	public SearchResponse call() throws Exception {
		if(indexPattern != null && typePattern != null) {
			return searchService.search(indexPattern, typePattern, requestBody);
		} else if(indexPattern != null) {
			return searchService.search(indexPattern, requestBody);
		} else {
			return searchService.search(requestBody);
		}
	}

}
