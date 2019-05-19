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
package com.elefana.table;

import com.elefana.api.indices.IndexGenerationMode;

public class TableIndexDelay {
	private final String tableName;
	private final long indexTimestamp;
	private final IndexGenerationMode mode;

	public TableIndexDelay(String tableName, long indexTimestamp, IndexGenerationMode mode) {
		this.tableName = tableName;
		this.indexTimestamp = indexTimestamp;
		this.mode = mode;
	}

	public String getTableName() {
		return tableName;
	}

	public long getIndexTimestamp() {
		return indexTimestamp;
	}

	public IndexGenerationMode getMode() {
		return mode;
	}
}
