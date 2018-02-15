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
package com.elefana.indices.local;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.elefana.cluster.AckResponse;
import com.elefana.indices.PutIndexTemplateRequest;
import com.elefana.indices.PutIndexTemplateResponse;

public class LocalPutIndexTemplateRequest extends PutIndexTemplateRequest implements Callable<PutIndexTemplateResponse> {
	private final LocalIndexTemplateService indexTemplateService;

	public LocalPutIndexTemplateRequest(LocalIndexTemplateService indexTemplateService, String templateId, String requestBody) {
		super(null, templateId, requestBody);
		this.indexTemplateService = indexTemplateService;
	}

	@Override
	public void execute() {
		if(this.responseFuture != null) {
			return;
		}
		FutureTask<PutIndexTemplateResponse> responseFuture = new FutureTask<PutIndexTemplateResponse>(this);
		responseFuture.run();
		this.responseFuture = responseFuture;
	}

	@Override
	public PutIndexTemplateResponse call() throws Exception {
		AckResponse response = indexTemplateService.putIndexTemplate(templateId, requestBody);
		return new PutIndexTemplateResponse(response.isAcknowledged());
	}

}
