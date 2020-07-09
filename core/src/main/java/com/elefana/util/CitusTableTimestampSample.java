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

public class CitusTableTimestampSample extends UniqueSelfDescribingMarshallable implements Comparable<CitusTableTimestampSample>, BytesMarshallable {
	private String indexName;
	private String tableName;
	private long timestampSample;

	public CitusTableTimestampSample() {}

	public CitusTableTimestampSample(String indexName, String tableName, long timestampSample) {
		this.indexName = indexName;
		this.tableName = tableName;
		this.timestampSample = timestampSample;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		bytes.writeUtf8(indexName);
		bytes.writeUtf8(tableName);
		bytes.writeLong(timestampSample);
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		indexName = bytes.readUtf8();
		tableName = bytes.readUtf8();
		timestampSample = bytes.readLong();
	}

	public String getIndexName() {
		return indexName;
	}

	public String getTableName() {
		return tableName;
	}

	public long getTimestampSample() {
		return timestampSample;
	}

	@Override
	public int compareTo(CitusTableTimestampSample o) {
		return tableName.compareTo(o.tableName);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof CitusTableTimestampSample)) return false;
		CitusTableTimestampSample that = (CitusTableTimestampSample) o;
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
