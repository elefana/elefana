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

import com.elefana.api.document.IndexRequest;
import com.elefana.api.document.IndexResponse;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Callable;

public class PsqlIndexRequest extends IndexRequest implements Callable<IndexResponse> {
	private final PsqlDocumentService documentService;

	public PsqlIndexRequest(PsqlDocumentService documentService, ChannelHandlerContext context) {
		super(documentService, context);
		this.documentService = documentService;
	}

	@Override
	protected Callable<IndexResponse> internalExecute() {
		return this;
	}

	@Override
	public IndexResponse call() throws Exception {
		return documentService.index(getIndex(), getType(), getId(), getSource(), getOpType());
	}
}
