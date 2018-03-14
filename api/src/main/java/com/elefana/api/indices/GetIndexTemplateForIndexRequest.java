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

import java.util.concurrent.Callable;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;

public abstract class GetIndexTemplateForIndexRequest extends ApiRequest<GetIndexTemplateForIndexResponse> implements Callable<GetIndexTemplateForIndexResponse> {
	private final String index;
	
	public GetIndexTemplateForIndexRequest(RequestExecutor requestExecutor, String index) {
		super(requestExecutor);
		this.index = index;
	}
	
	@Override
	protected Callable<GetIndexTemplateForIndexResponse> internalExecute() {
		return this;
	}

	public String getIndex() {
		return index;
	}

}
