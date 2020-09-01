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

import com.elefana.api.indices.GetFieldNamesRequest;
import com.elefana.api.indices.GetFieldNamesResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlGetFieldNamesRequest extends GetFieldNamesRequest implements Callable<GetFieldNamesResponse> {
	private final PsqlIndexFieldMappingService indexFieldMappingService;

	public PsqlGetFieldNamesRequest(PsqlIndexFieldMappingService indexFieldMappingService, ChannelHandlerContext context,
	                                String indexPattern) {
		this(indexFieldMappingService, context, indexPattern, "*");
	}

	public PsqlGetFieldNamesRequest(PsqlIndexFieldMappingService indexFieldMappingService, ChannelHandlerContext context,
	                                String indexPattern, String typePattern) {
		super(indexFieldMappingService, context, indexPattern, typePattern);
		this.indexFieldMappingService = indexFieldMappingService;
	}

	@Override
	protected Callable<GetFieldNamesResponse> internalExecute() {
		return this;
	}

	@Override
	public GetFieldNamesResponse call() throws Exception {
		return indexFieldMappingService.getFieldNames(this);
	}
}
