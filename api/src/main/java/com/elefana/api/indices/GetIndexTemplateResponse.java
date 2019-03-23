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

import com.elefana.api.ApiResponse;
import com.elefana.api.json.GetIndexTemplateForIndexResponseDecoder;
import com.elefana.api.json.GetIndexTemplateResponseDecoder;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

public class GetIndexTemplateResponse extends ApiResponse {
	static {
		JsoniterSpi.registerTypeDecoder(GetIndexTemplateResponse.class, new GetIndexTemplateResponseDecoder());
	}

	protected String templateId;
	protected final Map<String, IndexTemplate> templates = new HashMap<String, IndexTemplate>(1);

	public GetIndexTemplateResponse() {
		super(HttpResponseStatus.OK.code());
	}

	public GetIndexTemplateResponse(String templateId) {
		super(HttpResponseStatus.OK.code());
		this.templateId = templateId;
	}
	
	@Override
	public String toJsonString() {
		return JsonStream.serialize(templates);
	}

	public Map<String, IndexTemplate> getTemplates() {
		return templates;
	}

	public IndexTemplate getIndexTemplate() {
		if(templateId == null) {
			return null;
		}
		return templates.get(templateId);
	}

	public void setIndexTemplate(IndexTemplate indexTemplate) {
		if(templateId == null) {
			return;
		}
		templates.put(templateId, indexTemplate);
	}

	public String getTemplateId() {
		return templateId;
	}

	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((templateId == null) ? 0 : templateId.hashCode());
		result = prime * result + ((templates == null) ? 0 : templates.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GetIndexTemplateResponse other = (GetIndexTemplateResponse) obj;
		if (templateId == null) {
			if (other.templateId != null)
				return false;
		} else if (!templateId.equals(other.templateId))
			return false;
		if (templates == null) {
			if (other.templates != null)
				return false;
		} else if (!templates.equals(other.templates))
			return false;
		return true;
	}
}
