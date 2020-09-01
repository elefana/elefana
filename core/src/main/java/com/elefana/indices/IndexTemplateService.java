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
package com.elefana.indices;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.indices.*;
import com.elefana.api.util.PooledStringBuilder;
import io.netty.channel.ChannelHandlerContext;

public interface IndexTemplateService {
	
	public ListIndexTemplatesRequest prepareListIndexTemplates(ChannelHandlerContext context, String ... templateIds);
	
	public GetIndexTemplateForIndexRequest prepareGetIndexTemplateForIndex(ChannelHandlerContext context, String index);
	
	public GetIndexTemplateRequest prepareGetIndexTemplate(ChannelHandlerContext context, String templateId, boolean fetchSource);
	
	public PutIndexTemplateRequest preparePutIndexTemplate(ChannelHandlerContext context, String templateId, PooledStringBuilder requestBody);

	public IndexTemplate getIndexTemplateForIndex(String index)  throws ElefanaException;
}
