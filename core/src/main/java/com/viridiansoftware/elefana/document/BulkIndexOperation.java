/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.document;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BulkIndexOperation {
	private static final Queue<BulkIndexOperation> POOL = new ConcurrentLinkedQueue<BulkIndexOperation>();
	
	private String index;
	private String type;
	private String id;
	private String source;
	
	public static BulkIndexOperation allocate() {
		BulkIndexOperation result = POOL.poll();
		if(result == null) {
			return new BulkIndexOperation();
		}
		return result;
	}	
	
	public void release() {
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
}
