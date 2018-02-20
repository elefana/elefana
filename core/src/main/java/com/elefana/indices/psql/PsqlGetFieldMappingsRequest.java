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

import com.elefana.api.indices.GetFieldMappingsRequest;
import com.elefana.api.indices.GetFieldMappingsResponse;

public class PsqlGetFieldMappingsRequest extends GetFieldMappingsRequest implements Callable<GetFieldMappingsResponse> {
	private final PsqlIndexFieldMappingService indexFieldMappingService;

	public PsqlGetFieldMappingsRequest(PsqlIndexFieldMappingService indexFieldMappingService) {
		super(indexFieldMappingService);
		this.indexFieldMappingService = indexFieldMappingService;
	}

	@Override
	protected Callable<GetFieldMappingsResponse> internalExecute() {
		return this;
	}

	@Override
	public GetFieldMappingsResponse call() throws Exception {
		if(getTypesPattern() != null) {
			return indexFieldMappingService.getMapping(getIndicesPattern(), getTypesPattern());
		} else {
			return indexFieldMappingService.getMapping(getIndicesPattern());
		}
	}

}
