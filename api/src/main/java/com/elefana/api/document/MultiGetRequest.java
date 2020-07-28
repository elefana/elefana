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
import com.elefana.api.util.PooledStringBuilder;

import java.util.ArrayList;
import java.util.List;

public abstract class MultiGetRequest extends ApiRequest<MultiGetResponse> {
	private final List<GetRequest> getRequests = new ArrayList<GetRequest>(1);
	protected String indexPattern, typePattern;
	private PooledStringBuilder requestBody;
	
	public MultiGetRequest(RequestExecutor requestExecutor) {
		super(requestExecutor);
	}

	public MultiGetRequest(RequestExecutor requestExecutor, PooledStringBuilder requestBody) {
		super(requestExecutor);
		this.requestBody = requestBody;
	}
	
	public void add(GetRequest getRequest) {
		getRequests.add(getRequest);
		this.requestBody = null;
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
		StringBuilder result = new StringBuilder();
		if(requestBody != null) {
			result.append(requestBody);
		} else {
			result.append('{');
			result.append("\"docs\":[");
			for(int i = 0; i < getRequests.size(); i++) {
				GetRequest getRequest = getRequests.get(i);
				result.append('{');
				if(getRequest.getIndex() != null) {
					result.append("\"_index\":\"");
					result.append(getRequest.getIndex());
					result.append('\"');
				}
				if(getRequest.getType() != null) {
					if(getRequest.getIndex() != null) {
						result.append(',');
					}
					
					result.append("\"_type\":\"");
					result.append(getRequest.getType());
					result.append('\"');
				}
				if(getRequest.getId() != null) {
					if(getRequest.getIndex() != null || getRequest.getType() != null) {
						result.append(',');
					}
					
					result.append("\"_id\":\"");
					result.append(getRequest.getId());
					result.append('\"');
				}
				result.append('}');
				
				if(i < getRequests.size() - 1) {
					result.append(',');
				}
			}
			result.append(']');
			result.append('}');
		}
		return result.toString();
	}

	public void setRequestBody(PooledStringBuilder requestBody) {
		if(requestBody == null) {
			return;
		}
		this.requestBody = requestBody;
	}

}
