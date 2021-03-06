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

import com.elefana.api.document.MultiGetRequest;
import com.elefana.api.document.MultiGetResponse;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlMultiGetRequest extends MultiGetRequest implements Callable<MultiGetResponse> {
	private final PsqlDocumentService documentService;
	
	public PsqlMultiGetRequest(PsqlDocumentService documentService, ChannelHandlerContext context,
	                           PooledStringBuilder requestBody) {
		super(documentService, context, requestBody);
		this.documentService = documentService;
	}

	@Override
	protected Callable<MultiGetResponse> internalExecute() {
		return this;
	}

	@Override
	public MultiGetResponse call() throws Exception {
		if(indexPattern != null && typePattern != null) {
			return documentService.multiGet(indexPattern, typePattern, getRequestBody());
		} else if(indexPattern != null) {
			return documentService.multiGet(indexPattern, getRequestBody());
		} else {
			return documentService.multiGet(getRequestBody());
		}
	}

}
