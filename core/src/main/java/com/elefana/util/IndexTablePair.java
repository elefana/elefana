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
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.Objects;

public class IndexTablePair extends UniqueSelfDescribingMarshallable implements Comparable<IndexTablePair>, BytesMarshallable {
	private String indexName;
	private String tableName;

	public IndexTablePair() {}

	public IndexTablePair(String indexName, String tableName, long timestampSample) {
		this.indexName = indexName;
		this.tableName = tableName;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		bytes.writeUtf8(indexName);
		bytes.writeUtf8(tableName);
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		indexName = bytes.readUtf8();
		tableName = bytes.readUtf8();
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public int compareTo(IndexTablePair o) {
		return tableName.compareTo(o.tableName);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof IndexTablePair)) return false;
		IndexTablePair that = (IndexTablePair) o;
		return Objects.equals(tableName, that.tableName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableName);
	}

	@Override
	public String getKey() {
		return tableName;
	}
}
