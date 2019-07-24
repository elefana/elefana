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

import com.elefana.api.exception.ShardFailedException;
import com.elefana.api.indices.IndexTemplate;

import java.util.List;

/**
 *
 */
public interface SearchQueryBuilder {
	public static final String SEARCH_TABLE_PREFIX = "elefana_search_";

	public PsqlQueryComponents buildQuery(IndexTemplate matchedIndexTemplate, List<String> indices, String[] types,
			RequestBodySearch requestBodySearch) throws ShardFailedException;

	public static ThreadLocal<StringBuilder> POOLED_STRING_BUILDER = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder(512);
		}

		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0); // clear/reset the buffer
			return b;
		}
	};
}
