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
package com.elefana.search;

import com.elefana.api.search.MultiSearchRequest;
import com.elefana.api.search.SearchRequest;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

public interface SearchService {
	
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, PooledStringBuilder requestBody);
	
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, String fallbackIndex, PooledStringBuilder requestBody);
	
	public MultiSearchRequest prepareMultiSearch(ChannelHandlerContext context, String fallbackIndex, String fallbackType, PooledStringBuilder requestBody);

	public SearchRequest prepareSearch(ChannelHandlerContext context, PooledStringBuilder requestBody);

	public SearchRequest prepareSearch(ChannelHandlerContext context, String indexPattern, PooledStringBuilder requestBody);

	public SearchRequest prepareSearch(ChannelHandlerContext context, String indexPattern, String typesPattern, PooledStringBuilder requestBody);
}
