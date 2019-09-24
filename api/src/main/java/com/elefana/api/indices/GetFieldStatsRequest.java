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

import java.util.List;

public abstract class GetFieldStatsRequest extends ApiRequest<GetFieldStatsResponse> {
	protected final String indexPattern;
	protected final boolean clusterLevel;
	protected final List<String> fields;

	public GetFieldStatsRequest(RequestExecutor requestExecutor, String indexPattern, List<String> fields, boolean clusterLevel) {
		super(requestExecutor);
		this.indexPattern = indexPattern;
		this.clusterLevel = clusterLevel;
		this.fields = fields;
	}

	public String getIndexPattern() {
		return indexPattern;
	}

	public boolean getClusterLevel() {
		return clusterLevel;
	}

	public List<String> getFields() {
		return fields;
	}
}
