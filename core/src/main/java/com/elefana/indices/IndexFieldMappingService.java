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

import com.elefana.api.indices.*;
import com.elefana.api.util.PooledStringBuilder;

public interface IndexFieldMappingService {
	
	public GetFieldMappingsRequest prepareGetFieldMappings();
	
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern);
	
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern, String typePattern);
	
	public GetFieldMappingsRequest prepareGetFieldMappings(String indexPattern, String typePattern, String fieldPattern);
	
	public PutFieldMappingRequest preparePutFieldMappings(String index, PooledStringBuilder mappings);
	
	public PutFieldMappingRequest preparePutFieldMappings(String index, String type, PooledStringBuilder mappings);
	
	public GetFieldCapabilitiesRequest prepareGetFieldCapabilities(String indexPattern);

	public RefreshIndexRequest prepareRefreshIndex(String index);
}
