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
package com.elefana.indices;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.elefana.api.ApiRequest;

public abstract class PutIndexTemplateRequest extends ApiRequest<PutIndexTemplateResponse> implements Callable<PutIndexTemplateResponse> {
	protected final String templateId;
	protected final String requestBody;

	public PutIndexTemplateRequest(ExecutorService executorService, String templateId, String requestBody) {
		super(executorService);
		this.templateId = templateId;
		this.requestBody = requestBody;
	}
	
	@Override
	protected Callable<PutIndexTemplateResponse> internalExecute() {
		return this;
	}

	public String getRequestBody() {
		return requestBody;
	}

	public String getTemplateId() {
		return templateId;
	}

}
