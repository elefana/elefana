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
package com.elefana.indices.fieldstats;

import com.elefana.api.indices.GetFieldNamesRequest;
import com.elefana.api.indices.GetFieldNamesResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class RealtimeIndexFieldNamesRequest extends GetFieldNamesRequest {
	private final IndexFieldStatsService indexFieldStatsService;

	public RealtimeIndexFieldNamesRequest(IndexFieldStatsService indexFieldStatsService, ChannelHandlerContext context,
	                                      String indexPattern, String typePattern) {
		super(indexFieldStatsService, context, indexPattern, typePattern);
		this.indexFieldStatsService = indexFieldStatsService;
	}

	@Override
	protected Callable<GetFieldNamesResponse> internalExecute() {
		return () -> indexFieldStatsService.getFieldNames(context, indexPattern, typePattern);
	}
}
