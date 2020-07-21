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

import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.fieldstats.job.DocumentSourceProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkIndexOperation implements DocumentSourceProvider {
	@JsonIgnore
	private static final Queue<BulkIndexOperation> POOL = new ConcurrentLinkedQueue<BulkIndexOperation>();
	private static AtomicInteger MAX_SOURCE_LENGTH = new AtomicInteger(2048);
	
	private String index;
	private String type;
	private String id;
	private char [] document = new char[MAX_SOURCE_LENGTH.get()];
	private int documentLength;
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
		documentLength = 0;

		released = true;
		POOL.offer(this);
	}

	public void read(JsonParser jsonParser) throws IOException {
		while(jsonParser.currentToken() != JsonToken.END_OBJECT) {
			final JsonToken nextToken = jsonParser.nextToken();

			if(nextToken != JsonToken.FIELD_NAME) {
				continue;
			}
			switch(jsonParser.getText()) {
			case "_index":
				jsonParser.nextToken();
				index = jsonParser.getText();
				break;
			case "_type":
				jsonParser.nextToken();
				type = jsonParser.getText();
				break;
			case "_id":
				jsonParser.nextToken();
				id = jsonParser.getText();
				break;
			}
		}
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

	public void setDocument(char [] document, int documentLength) {
		if(this.document.length < documentLength) {
			MAX_SOURCE_LENGTH.set(Math.max(documentLength, MAX_SOURCE_LENGTH.get()));
			this.document = new char[MAX_SOURCE_LENGTH.get()];
		}

		System.arraycopy(document, 0, this.document, 0, documentLength);
		this.documentLength = documentLength;
	}

	@Override
	public void setDocument(StringBuilder builder) {
		if(this.document.length < builder.length()) {
			MAX_SOURCE_LENGTH.set(Math.max(builder.length(), MAX_SOURCE_LENGTH.get()));
			this.document = new char[MAX_SOURCE_LENGTH.get()];
		}

		builder.getChars(0, builder.length(), this.document, 0);
		this.documentLength = builder.length();
	}

	@Override
	public void setDocument(PooledStringBuilder builder) {
		if(this.document.length < builder.length()) {
			MAX_SOURCE_LENGTH.set(Math.max(builder.length(), MAX_SOURCE_LENGTH.get()));
			this.document = new char[MAX_SOURCE_LENGTH.get()];
		}

		builder.getChars(0, builder.length(), this.document, 0);
		this.documentLength = builder.length();
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
		return "BulkIndexOperation [index=" + index + ", type=" + type + ", id=" + id + ", source=" + document + "]";
	}

	@Override
	public char[] getDocument() {
		return document;
	}

	@Override
	public int getDocumentLength() {
		return documentLength;
	}

	public void setDocumentLength(int documentLength) {
		this.documentLength = documentLength;
	}

	@Override
	public void dispose() {
		release();
	}
}
