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

import java.util.concurrent.FutureTask;

import com.elefana.indices.GetIndexTemplateRequest;
import com.elefana.indices.GetIndexTemplateResponse;

public class LocalGetIndexTemplateRequest extends GetIndexTemplateRequest {
	private final LocalIndexTemplateService indexTemplateService;
	private final String [] templateIds;

	public LocalGetIndexTemplateRequest(LocalIndexTemplateService indexTemplateService, String... templateIds) {
		super(null);
		this.indexTemplateService = indexTemplateService;
		this.templateIds = templateIds;
	}

	@Override
	public void execute() {
		if(this.responseFuture != null) {
			return;
		}
		FutureTask<GetIndexTemplateResponse> responseFuture = new FutureTask<GetIndexTemplateResponse>(this);
		responseFuture.run();
		this.responseFuture = responseFuture;
	}

	@Override
	public GetIndexTemplateResponse call() throws Exception {
		GetIndexTemplateResponse result = new GetIndexTemplateResponse();
		if(templateIds == null || templateIds.length == 0) {
			result.getTemplates().addAll(indexTemplateService.getIndexTemplates());
		} else {
			for(String templateId : templateIds) {
				result.getTemplates().add(indexTemplateService.getIndexTemplate(templateId));
			}
		}
		return result;
	}
}
