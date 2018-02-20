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
package com.elefana.api.document;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;

public abstract class MultiGetRequest extends ApiRequest<MultiGetResponse> {
	protected String indexPattern, typePattern, requestBody;
	
	public MultiGetRequest(RequestExecutor requestExecutor) {
		super(requestExecutor);
	}

	public MultiGetRequest(RequestExecutor requestExecutor, String requestBody) {
		super(requestExecutor);
		this.requestBody = requestBody;
	}

	public String getIndexPattern() {
		return indexPattern;
	}

	public void setIndexPattern(String indexPattern) {
		if(indexPattern == null) {
			return;
		}
		this.indexPattern = indexPattern;
	}

	public String getTypePattern() {
		return typePattern;
	}

	public void setTypePattern(String typePattern) {
		if(typePattern == null) {
			return;
		}
		this.typePattern = typePattern;
	}

	public String getRequestBody() {
		return requestBody;
	}

	public void setRequestBody(String requestBody) {
		if(requestBody == null) {
			return;
		}
		this.requestBody = requestBody;
	}

}
