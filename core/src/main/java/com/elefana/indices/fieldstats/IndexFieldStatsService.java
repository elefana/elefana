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

import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.indices.GetFieldStatsRequest;
import com.elefana.api.indices.GetFieldStatsResponse;
import com.elefana.document.BulkIndexOperation;
import com.jsoniter.any.Any;

import java.util.List;

public interface IndexFieldStatsService {

    public GetFieldStatsRequest prepareGetFieldStatsPost(String indexPattern, String requestBody, boolean clusterLevel) throws NoSuchApiException;

    public GetFieldStatsRequest prepareGetFieldStatsGet(String indexPattern, String fieldGetParam, boolean clusterLevel);

    public GetFieldStatsResponse getFieldStats(String indexPattern, List<String> fields, boolean clusterLevel);

    public void submitDocument(Any document, String index);
    public void submitDocument(String document, String index);
    public void submitDocuments(List<BulkIndexOperation> documents);

    public void deleteIndex(String index);
}
