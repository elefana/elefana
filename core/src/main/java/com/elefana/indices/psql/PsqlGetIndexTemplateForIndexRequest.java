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

import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;

import java.util.concurrent.Callable;

public class PsqlGetIndexTemplateForIndexRequest extends GetIndexTemplateForIndexRequest implements Callable<GetIndexTemplateForIndexResponse> {
	private final PsqlIndexTemplateService indexTemplateService;

	public PsqlGetIndexTemplateForIndexRequest(PsqlIndexTemplateService indexTemplateService, String index) {
		super(indexTemplateService, index);
		this.indexTemplateService = indexTemplateService;
	}

	@Override
	public GetIndexTemplateForIndexResponse call() throws Exception {
		IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(getIndex());
		GetIndexTemplateForIndexResponse result = new GetIndexTemplateForIndexResponse(getIndex(), indexTemplate != null ? indexTemplate.getTemplateId() : null);
		if(indexTemplate != null) {
			result.setIndexTemplate(indexTemplate);
		}
		return result;
	}

	@Override
	protected Callable<GetIndexTemplateForIndexResponse> internalExecute() {
		return this;
	}
}
