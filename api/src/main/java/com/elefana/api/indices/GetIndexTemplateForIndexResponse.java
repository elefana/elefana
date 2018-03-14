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

import java.util.HashMap;
import java.util.Map;

import com.elefana.api.json.GetIndexTemplateForIndexResponseDecoder;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;

public class GetIndexTemplateForIndexResponse extends GetIndexTemplateResponse {
	static {
		JsoniterSpi.registerTypeDecoder(GetIndexTemplateForIndexResponse.class, new GetIndexTemplateForIndexResponseDecoder());
	}
	
	private final String index;

	public GetIndexTemplateForIndexResponse(String index, String templateId) {
		super(templateId);
		this.index = index;
	}
	
	@Override
	public String toJsonString() {
		final Map<String, Object> result = new HashMap<String, Object>();
		result.put("index", index);
		result.put("templateId", templateId);
		result.put("template", getIndexTemplate());
		return JsonStream.serialize(result);
	}

	public String getIndex() {
		return index;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		GetIndexTemplateForIndexResponse other = (GetIndexTemplateForIndexResponse) obj;
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
		if (index == null) {
			if (other.index != null)
				return false;
		} else if (!index.equals(other.index))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "GetIndexTemplateForIndexResponse [toJsonString()=" + toJsonString() + "]";
	}
}
