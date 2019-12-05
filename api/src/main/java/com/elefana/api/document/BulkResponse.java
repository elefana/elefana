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

import com.elefana.api.ApiResponse;
import com.elefana.api.json.JsonUtils;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;

public class BulkResponse extends ApiResponse {	

	private long took;
	private boolean errors;
	private final List<BulkItemResponse> items;
	
	public BulkResponse() {
		super(HttpResponseStatus.OK.code());
		items = new ArrayList<BulkItemResponse>();
	}

	public BulkResponse(int capacity) {
		super(HttpResponseStatus.OK.code());
		items = new ArrayList<BulkItemResponse>(capacity);
	}

	public long getTook() {
		return took;
	}

	public void setTook(long took) {
		this.took = took;
	}

	public boolean isErrors() {
		return errors;
	}

	public void setErrors(boolean errors) {
		this.errors = errors;
	}

	public List<BulkItemResponse> getItems() {
		return items;
	}

	@Override
	public String toJsonString() {
		return JsonUtils.toJsonString(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (errors ? 1231 : 1237);
		result = prime * result + ((items == null) ? 0 : items.hashCode());
		result = prime * result + (int) (took ^ (took >>> 32));
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
		BulkResponse other = (BulkResponse) obj;
		if (errors != other.errors)
			return false;
		if (items == null) {
			if (other.items != null)
				return false;
		} else if (!items.equals(other.items))
			return false;
		if (took != other.took)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BulkResponse [took=" + took + ", errors=" + errors + ", items=" + items + "]";
	}
}
