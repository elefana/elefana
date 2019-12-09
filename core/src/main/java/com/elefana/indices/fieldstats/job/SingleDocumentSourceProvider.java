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

public class SingleDocumentSourceProvider implements DocumentSourceProvider {
	private char [] document;
	private int documentLength;

	public SingleDocumentSourceProvider(String document) {
		this.document = document.toCharArray();
		this.documentLength = document.length();
	}

	public SingleDocumentSourceProvider(char[] document, int documentLength) {
		this.document = document;
		this.documentLength = documentLength;
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
		document = null;
	}
}
