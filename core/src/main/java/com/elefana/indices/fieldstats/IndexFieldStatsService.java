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

import com.elefana.api.RequestExecutor;
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.indices.GetFieldNamesRequest;
import com.elefana.api.indices.GetFieldNamesResponse;
import com.elefana.api.indices.GetFieldStatsRequest;
import com.elefana.api.indices.GetFieldStatsResponse;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.document.BulkIndexOperation;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public interface IndexFieldStatsService extends RequestExecutor {

    public GetFieldStatsRequest prepareGetFieldStatsPost(ChannelHandlerContext context,
                                                         String indexPattern, PooledStringBuilder requestBody,
                                                         boolean clusterLevel) throws NoSuchApiException;

    public GetFieldStatsRequest prepareGetFieldStatsGet(ChannelHandlerContext context,
                                                        String indexPattern, String fieldGetParam, boolean clusterLevel);

    public GetFieldStatsResponse getFieldStats(ChannelHandlerContext context,
                                               String indexPattern, List<String> fields, boolean clusterLevel);

    public GetFieldNamesRequest prepareGetFieldNames(ChannelHandlerContext context, String indexPattern);

    public GetFieldNamesRequest prepareGetFieldNames(ChannelHandlerContext context, String indexPattern, String typePattern);

    public GetFieldNamesResponse getFieldNames(ChannelHandlerContext context, String indexPattern, String typePattern);

    public boolean hasField(String index, String field);

    public boolean isBooleanField(String index, String field);

    public boolean isDateField(String index, String field);

    public boolean isDoubleField(String index, String field);

    public boolean isLongField(String index, String field);

    public boolean isStringField(String index, String field);

    public void submitDocument(String document, String index);

    public void submitDocument(PooledStringBuilder document, String index);

    public void submitDocuments(List<BulkIndexOperation> documents, int from, int size);

    public void deleteIndex(String index);
}
