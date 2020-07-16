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
package com.elefana.indices.fieldstats.job;

import com.elefana.api.util.PooledStringBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SingleDocumentSourceProvider implements DocumentSourceProvider {
	private static final Lock LOCK = new ReentrantLock();
	private static final List<SingleDocumentSourceProvider> POOL = new ArrayList<SingleDocumentSourceProvider>(32);

	private char [] document;
	private int documentLength;

	private void set(String document) {
		if(this.document == null || this.document.length < document.length()) {
			this.document = new char[document.length()];
		}
		document.getChars(0, document.length(), this.document, 0);
		this.documentLength = document.length();
	}

	private void set(PooledStringBuilder document) {
		if(this.document == null || this.document.length < document.length()) {
			this.document = new char[document.length()];
		}
		this.documentLength = document.length();
		document.getChars(this.document);
	}

	private void set(char[] document, int documentLength) {
		if(this.document == null || this.document.length < document.length) {
			this.document = new char[document.length];
		}
		this.documentLength = documentLength;
		System.arraycopy(document, 0, this.document, 0, documentLength);
	}

	@Override
	public char [] getDocument() {
		return document;
	}

	@Override
	public void setDocument(char[] document, int length) {
		this.document = document;
		this.documentLength = length;
	}

	@Override
	public void setDocument(StringBuilder builder) {
		if(this.document.length < builder.length()) {
			this.document = new char[builder.length()];
		}

		builder.getChars(0, builder.length(), this.document, 0);
		this.documentLength = builder.length();
	}

	@Override
	public int getDocumentLength() {
		return documentLength;
	}

	@Override
	public void dispose() {
		LOCK.lock();
		POOL.add(this);
		LOCK.unlock();
	}

	public static SingleDocumentSourceProvider allocate(String document) {
		final SingleDocumentSourceProvider result;
		LOCK.lock();
		if(POOL.isEmpty()) {
			result = new SingleDocumentSourceProvider();
		} else {
			result = POOL.remove(0);
		}
		LOCK.unlock();
		result.set(document);
		return result;
	}

	public static SingleDocumentSourceProvider allocate(PooledStringBuilder document) {
		final SingleDocumentSourceProvider result;
		LOCK.lock();
		if(POOL.isEmpty()) {
			result = new SingleDocumentSourceProvider();
		} else {
			result = POOL.remove(0);
		}
		LOCK.unlock();
		result.set(document);
		return result;
	}

	public static SingleDocumentSourceProvider allocate(char[] document, int documentLength) {
		final SingleDocumentSourceProvider result;
		LOCK.lock();
		if(POOL.isEmpty()) {
			result = new SingleDocumentSourceProvider();
		} else {
			result = POOL.remove(0);
		}
		LOCK.unlock();
		result.set(document, documentLength);
		return result;
	}
}
