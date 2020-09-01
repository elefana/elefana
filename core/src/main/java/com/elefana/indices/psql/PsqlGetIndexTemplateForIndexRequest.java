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
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlGetIndexTemplateForIndexRequest extends GetIndexTemplateForIndexRequest implements Callable<GetIndexTemplateForIndexResponse> {
	private final PsqlIndexTemplateService indexTemplateService;

	public PsqlGetIndexTemplateForIndexRequest(PsqlIndexTemplateService indexTemplateService, ChannelHandlerContext context,
	                                           String index) {
		super(indexTemplateService, context, index);
		this.indexTemplateService = indexTemplateService;
	}

	@Override
	public GetIndexTemplateForIndexResponse call() throws Exception {
		final String templateId = indexTemplateService.getIndexTemplateIdForIndex(getIndex());
		if(templateId == null) {
			return new GetIndexTemplateForIndexResponse(getIndex(), null);
		}
		IndexTemplate indexTemplate = indexTemplateService.getIndexTemplate(templateId, true);
		GetIndexTemplateForIndexResponse result = new GetIndexTemplateForIndexResponse(getIndex(), templateId);
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
