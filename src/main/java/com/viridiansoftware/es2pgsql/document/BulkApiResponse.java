/**
 * Copyright 2017 Viridian Software Ltd.
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
 */
package com.viridiansoftware.es2pgsql.document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkApiResponse {
	private long took;
	private boolean errors;
	private final List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
	
	public Map<String, Object> appendEntry(String operation, String index, String type, String id) {
		final Map<String, Object> entry = new HashMap<String, Object>();
		final Map<String, Object> entryData = new HashMap<String, Object>();
		
		entryData.put("_index", index);
		entryData.put("_type", type);
		entryData.put("_id", id);
		entryData.put("_version", 1);
		
		final Map<String, Object> shards = new HashMap<String, Object>();
		shards.put("total", 1);
		shards.put("successful", 1);
		shards.put("failed", 0);
		entryData.put("_shards", shards);
		
		entry.put(operation, entryData);
		
		items.add(entry);
		return entryData;
	}

	public long getTook() {
		return took;
	}

	public void setTook(long took) {
		this.took = took;
	}

	public boolean isErrors() {
		return errors;
	}

	public void setErrors(boolean errors) {
		this.errors = errors;
	}

	public List<Map<String, Object>> getItems() {
		return items;
	}
}
