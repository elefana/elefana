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

public abstract class GetRequest extends ApiRequest<GetResponse> {
	protected String index, type, id;

	public GetRequest(RequestExecutor requestExecutor, String index, String type, String id) {
		super(requestExecutor);
		this.index = index;
		this.type = type;
		this.id = id;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		if(index == null) {
			return;
		}
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		if(type == null) {
			return;
		}
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		if(id == null) {
			return;
		}
		this.id = id;
	}
}
