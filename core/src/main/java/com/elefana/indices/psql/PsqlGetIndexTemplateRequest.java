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

import com.elefana.api.indices.GetIndexTemplateRequest;
import com.elefana.api.indices.GetIndexTemplateResponse;

public class PsqlGetIndexTemplateRequest extends GetIndexTemplateRequest {
	private final PsqlIndexTemplateService indexTemplateService;

	public PsqlGetIndexTemplateRequest(PsqlIndexTemplateService indexTemplateService, String templateId) {
		super(indexTemplateService, templateId);
		this.indexTemplateService = indexTemplateService;
	}

	@Override
	public GetIndexTemplateResponse call() throws Exception {
		GetIndexTemplateResponse result = new GetIndexTemplateResponse(getTemplateId());
		result.setIndexTemplate(indexTemplateService.getIndexTemplate(getTemplateId(), isFetchSource()));
		return result;
	}
}
