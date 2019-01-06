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
package com.elefana.indices.psql;

import java.util.concurrent.FutureTask;

import com.elefana.api.indices.IndexTemplate;
import com.elefana.api.indices.ListIndexTemplatesRequest;
import com.elefana.api.indices.ListIndexTemplatesResponse;

public class PsqlListIndexTemplatesRequest extends ListIndexTemplatesRequest {
	private final PsqlIndexTemplateService indexTemplateService;
	private final String [] templateIds;

	public PsqlListIndexTemplatesRequest(PsqlIndexTemplateService indexTemplateService, String... templateIds) {
		super(null);
		this.indexTemplateService = indexTemplateService;
		this.templateIds = templateIds;
	}

	@Override
	public void execute() {
		if(this.responseFuture != null) {
			return;
		}
		FutureTask<ListIndexTemplatesResponse> responseFuture = new FutureTask<ListIndexTemplatesResponse>(this);
		responseFuture.run();
		this.responseFuture = responseFuture;
	}

	@Override
	public ListIndexTemplatesResponse call() throws Exception {
		ListIndexTemplatesResponse result = new ListIndexTemplatesResponse();
		if(templateIds == null || templateIds.length == 0) {
			for(IndexTemplate indexTemplate : indexTemplateService.getIndexTemplates()) {
				result.getTemplates().put(indexTemplate.getTemplateId(), indexTemplate);
			}
		} else {
			for(String templateId : templateIds) {
				result.getTemplates().put(templateId, indexTemplateService.getIndexTemplate(templateId, true));
			}
		}
		return result;
	}
}
