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

import com.elefana.api.RequestExecutor;
import com.elefana.api.document.DeleteRequest;
import com.elefana.api.document.DeleteResponse;

import java.util.concurrent.Callable;

public class PsqlDeleteRequest extends DeleteRequest {
	private final PsqlDocumentService documentService;

	public PsqlDeleteRequest(PsqlDocumentService documentService, String index, String type, String id) {
		super(documentService, index, type, id);
		this.documentService = documentService;
	}

	@Override
	protected Callable<DeleteResponse> internalExecute() {
		return null;
	}
}
