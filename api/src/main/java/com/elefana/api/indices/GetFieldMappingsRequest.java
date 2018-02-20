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

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;

public abstract class GetFieldMappingsRequest extends ApiRequest<GetFieldMappingsResponse> {
	private String indicesPattern;
	private String typesPattern;
	
	public GetFieldMappingsRequest(RequestExecutor requestExecutor) {
		super(requestExecutor);
		this.indicesPattern = "*";
		this.typesPattern = "*";
	}

	public String getIndicesPattern() {
		return indicesPattern;
	}

	public void setIndicesPattern(String indicesPattern) {
		if(indicesPattern == null) {
			return;
		}
		this.indicesPattern = indicesPattern;
	}

	public String getTypesPattern() {
		return typesPattern;
	}

	public void setTypesPattern(String typesPattern) {
		if(typesPattern == null) {
			return;
		}
		this.typesPattern = typesPattern;
	}
}
