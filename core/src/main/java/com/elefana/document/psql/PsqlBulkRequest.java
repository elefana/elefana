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
package com.elefana.document.psql;

import com.elefana.api.document.BulkRequest;
import com.elefana.api.document.BulkResponse;

import java.util.concurrent.Callable;

public class PsqlBulkRequest extends BulkRequest implements Callable<BulkResponse> {
	private final PsqlBulkIngestService bulkIngestService;

	public PsqlBulkRequest(PsqlBulkIngestService bulkIngestService, String requestBody) {
		super(bulkIngestService, requestBody);
		this.bulkIngestService = bulkIngestService;
	}

	@Override
	protected Callable<BulkResponse> internalExecute() {
		return this;
	}

	@Override
	public BulkResponse call() throws Exception {
		return bulkIngestService.bulkOperations(requestBody);
	}

}
