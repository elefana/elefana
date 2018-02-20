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

import com.elefana.api.indices.GetFieldStatsRequest;
import com.elefana.api.indices.GetFieldStatsResponse;

public class PsqlGetFieldStatsRequest extends GetFieldStatsRequest implements Callable<GetFieldStatsResponse> {
	private final PsqlIndexFieldMappingService indexFieldMappingService;

	public PsqlGetFieldStatsRequest(PsqlIndexFieldMappingService indexFieldMappingService, String index) {
		super(indexFieldMappingService, index);
		this.indexFieldMappingService = indexFieldMappingService;
	}

	@Override
	protected Callable<GetFieldStatsResponse> internalExecute() {
		return this;
	}

	@Override
	public GetFieldStatsResponse call() throws Exception {
		return indexFieldMappingService.getFieldStats(index);
	}

}