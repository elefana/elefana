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
package com.elefana.document.local;

import java.util.concurrent.Callable;

import com.elefana.document.IndexApiResponse;
import com.elefana.document.IndexRequest;

public class LocalIndexRequest extends IndexRequest implements Callable<IndexApiResponse> {
	private final LocalDocumentService documentService;

	public LocalIndexRequest(LocalDocumentService documentService) {
		super(documentService.getExecutorService());
		this.documentService = documentService;
	}

	@Override
	protected Callable<IndexApiResponse> internalExecute() {
		return this;
	}

	@Override
	public IndexApiResponse call() throws Exception {
		return documentService.index(getIndex(), getType(), getId(), getDocument(), getOpType());
	}
}
