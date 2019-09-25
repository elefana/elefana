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
package com.elefana.api.node;

import com.elefana.api.ApiRequest;
import com.elefana.api.RequestExecutor;

public abstract class NodesStatsRequest extends ApiRequest<NodesStatsResponse> {
	private boolean localOnly = false;
	private String [] filteredNodes;
	private String[] infoFields;

	public NodesStatsRequest(RequestExecutor requestExecutor) {
		super(requestExecutor);
	}

	public boolean isLocalOnly() {
		return localOnly;
	}

	public void setLocalOnly(boolean localOnly) {
		this.localOnly = localOnly;
	}

	public String[] getFilteredNodes() {
		return filteredNodes;
	}

	public void setFilteredNodes(String[] filteredNodes) {
		this.filteredNodes = filteredNodes;
	}

	public String[] getInfoFields() {
		return infoFields;
	}

	public void setInfoFields(String[] infoFields) {
		this.infoFields = infoFields;
	}

}
