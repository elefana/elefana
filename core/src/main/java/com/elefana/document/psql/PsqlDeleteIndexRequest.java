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

import com.elefana.api.AckResponse;
import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.DeleteIndexRequest;

import java.util.concurrent.Callable;

public class PsqlDeleteIndexRequest extends DeleteIndexRequest implements Callable<AckResponse> {
	private final PsqlDocumentService documentService;

	public PsqlDeleteIndexRequest(PsqlDocumentService documentService, String indexPattern) {
		this(documentService, indexPattern, "*");
	}

	public PsqlDeleteIndexRequest(PsqlDocumentService documentService, String indexPattern, String typePattern) {
		super(documentService, indexPattern, typePattern);
		this.documentService = documentService;
	}

	@Override
	protected Callable<AckResponse> internalExecute() {
		return this;
	}

	@Override
	public AckResponse call() throws Exception {
		return documentService.deleteIndex(getIndexPattern(), getTypePattern());
	}
}
