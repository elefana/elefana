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
package com.elefana.document;

import com.elefana.api.document.GetRequest;
import com.elefana.api.document.IndexOpType;
import com.elefana.api.document.IndexRequest;
import com.elefana.api.document.MultiGetRequest;

public interface DocumentService {

	public GetRequest prepareGet(String index, String type, String id, boolean fetchSource);

	public MultiGetRequest prepareMultiGet(String requestBody);

	public MultiGetRequest prepareMultiGet(String indexPattern, String requestBody);

	public MultiGetRequest prepareMultiGet(String indexPattern, String typePattern, String requestBody);

	public IndexRequest prepareIndex(String index, String type, String id, String document, IndexOpType opType);
}
