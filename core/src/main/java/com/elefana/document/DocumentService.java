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

import com.elefana.api.document.*;
import com.elefana.api.indices.DeleteIndexRequest;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

public interface DocumentService {

	public GetRequest prepareGet(ChannelHandlerContext context, String index, String type, String id, boolean fetchSource);

	public DeleteRequest prepareDelete(ChannelHandlerContext context, String index, String type, String id);

	public DeleteIndexRequest prepareDeleteIndex(ChannelHandlerContext context, String indexPattern, String typePattern, boolean async);

	public MultiGetRequest prepareMultiGet(ChannelHandlerContext context, PooledStringBuilder requestBody);

	public MultiGetRequest prepareMultiGet(ChannelHandlerContext context, String indexPattern, PooledStringBuilder requestBody);

	public MultiGetRequest prepareMultiGet(ChannelHandlerContext context, String indexPattern, String typePattern, PooledStringBuilder requestBody);

	public IndexRequest prepareIndex(ChannelHandlerContext context, String index, String type, String id, PooledStringBuilder document, IndexOpType opType);
}
