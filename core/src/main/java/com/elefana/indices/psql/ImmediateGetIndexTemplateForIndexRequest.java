package com.elefana.indices.psql;

import com.elefana.api.ImmediateRequestExecutor;
import com.elefana.api.RequestExecutor;
import com.elefana.api.indices.GetIndexTemplateForIndexRequest;
import com.elefana.api.indices.GetIndexTemplateForIndexResponse;
import com.elefana.api.indices.IndexTemplate;

import java.util.concurrent.Callable;

public class ImmediateGetIndexTemplateForIndexRequest extends GetIndexTemplateForIndexRequest implements Callable<GetIndexTemplateForIndexResponse> {
	private final IndexTemplate indexTemplate;
	private final String templateId;

	public ImmediateGetIndexTemplateForIndexRequest(String index, String templateId, IndexTemplate indexTemplate) {
		super(ImmediateRequestExecutor.INSTANCE, index);
		this.templateId = templateId;
		this.indexTemplate = indexTemplate;
	}

	@Override
	public GetIndexTemplateForIndexResponse call() throws Exception {
		final GetIndexTemplateForIndexResponse response = new GetIndexTemplateForIndexResponse(getIndex(), templateId);
		response.setIndexTemplate(indexTemplate);
		return response;
	}

	@Override
	protected Callable<GetIndexTemplateForIndexResponse> internalExecute() {
		return this;
	}
}
