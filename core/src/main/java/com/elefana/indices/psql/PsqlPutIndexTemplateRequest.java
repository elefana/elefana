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

import com.elefana.api.AckResponse;
import com.elefana.api.indices.PutIndexTemplateRequest;

import java.util.concurrent.Callable;

public class PsqlPutIndexTemplateRequest extends PutIndexTemplateRequest implements Callable<AckResponse> {
	private final PsqlIndexTemplateService indexTemplateService;

	public PsqlPutIndexTemplateRequest(PsqlIndexTemplateService indexTemplateService, String templateId, String requestBody) {
		super(indexTemplateService, templateId, requestBody);
		this.indexTemplateService = indexTemplateService;
	}

	@Override
	public AckResponse call() throws Exception {
		return indexTemplateService.putIndexTemplate(templateId, requestBody);
	}

	@Override
	protected Callable<AckResponse> internalExecute() {
		return this;
	}
}
