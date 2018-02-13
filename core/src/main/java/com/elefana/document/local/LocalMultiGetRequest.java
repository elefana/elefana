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

import com.elefana.document.MultiGetRequest;
import com.elefana.document.MultiGetResponse;

public class LocalMultiGetRequest extends MultiGetRequest implements Callable<MultiGetResponse> {
	private final LocalDocumentService documentService;
	
	public LocalMultiGetRequest(LocalDocumentService documentService, String requestBody) {
		super(documentService.getExecutorService(), requestBody);
		this.documentService = documentService;
	}

	@Override
	protected Callable<MultiGetResponse> internalExecute() {
		return this;
	}

	@Override
	public MultiGetResponse call() throws Exception {
		if(indexPattern != null && typePattern != null) {
			return documentService.multiGet(indexPattern, typePattern, requestBody);
		} else if(indexPattern != null) {
			return documentService.multiGet(indexPattern, requestBody);
		} else {
			return documentService.multiGet(requestBody);
		}
	}

}
