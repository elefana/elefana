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

import com.elefana.indices.fieldstats.job.DocumentSourceProvider;
import com.jsoniter.annotation.JsonIgnore;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BulkIndexOperation implements DocumentSourceProvider {
	@JsonIgnore
	private static final Queue<BulkIndexOperation> POOL = new ConcurrentLinkedQueue<BulkIndexOperation>();
	
	private String index;
	private String type;
	private String id;
	private String source;
	private long timestamp;

	@JsonIgnore
	private boolean released = false;
	
	@JsonIgnore
	public static BulkIndexOperation allocate() {
		BulkIndexOperation result = POOL.poll();
		if(result == null) {
			return new BulkIndexOperation();
		}
		result.released = false;
		return result;
	}	
	
	public void release() {
		if(released) {
			return;
		}
		index = null;
		type = null;
		id = null;
		source = null;

		released = true;
		POOL.offer(this);
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	@JsonIgnore
	public String toString() {
		return "BulkIndexOperation [index=" + index + ", type=" + type + ", id=" + id + ", source=" + source + "]";
	}

	@Override
	public String getDocument() {
		return source;
	}

	@Override
	public void dispose() {
		release();
	}
}
