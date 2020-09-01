/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

import com.elefana.api.document.BulkStatsRequest;
import com.elefana.api.document.BulkStatsResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlBulkStatsRequest extends BulkStatsRequest implements Callable<BulkStatsResponse> {
	private final PsqlBulkIngestService bulkIngestService;

	public PsqlBulkStatsRequest(PsqlBulkIngestService bulkIngestService, ChannelHandlerContext context) {
		super(bulkIngestService, context);
		this.bulkIngestService = bulkIngestService;
	}

	@Override
	protected Callable<BulkStatsResponse> internalExecute() {
		return this;
	}

	@Override
	public BulkStatsResponse call() throws Exception {
		return bulkIngestService.getBulkStats();
	}
}
