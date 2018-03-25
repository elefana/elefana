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
package com.elefana.indices.psql;

import java.util.concurrent.Callable;

import com.elefana.api.indices.RefreshIndexRequest;
import com.elefana.api.indices.RefreshIndexResponse;

public class PsqlRefreshIndexRequest extends RefreshIndexRequest implements Callable<RefreshIndexResponse> {
	private final PsqlIndexFieldMappingService indexFieldMappingService;
	
	public PsqlRefreshIndexRequest(PsqlIndexFieldMappingService indexFieldMappingService, String index) {
		super(indexFieldMappingService, index);
		this.indexFieldMappingService = indexFieldMappingService;
	}

	@Override
	protected Callable<RefreshIndexResponse> internalExecute() {
		return this;
	}

	@Override
	public RefreshIndexResponse call() throws Exception {
		indexFieldMappingService.scheduleIndexForMappingAndStats(getIndex());
		final RefreshIndexResponse result = new RefreshIndexResponse();
		result.getShards().put("total", 1);
		result.getShards().put("successful", 1);
		result.getShards().put("failed", 0);
		return result;
	}

}
