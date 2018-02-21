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

public abstract class PutIndexTemplateRequest extends ApiRequest<PutIndexTemplateResponse> implements Callable<PutIndexTemplateResponse> {
	protected final String templateId;
	protected String requestBody;
	
	public PutIndexTemplateRequest(RequestExecutor requestExecutor, String templateId) {
		super(requestExecutor);
		this.templateId = templateId;
		this.requestBody = "{}";
	}

	public PutIndexTemplateRequest(RequestExecutor requestExecutor, String templateId, String requestBody) {
		super(requestExecutor);
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

	public void setRequestBody(String requestBody) {
		if(requestBody == null) {
			return;
		}
		this.requestBody = requestBody;
	}

	public String getTemplateId() {
		return templateId;
	}

}
