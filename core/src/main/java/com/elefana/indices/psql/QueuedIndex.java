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
package com.elefana.indices.psql;

import com.elefana.util.UniqueSelfDescribingMarshallable;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.Objects;

public class QueuedIndex extends UniqueSelfDescribingMarshallable implements Comparable<QueuedIndex> {
	private String index;
	private long timestamp;

	public QueuedIndex() {}

	public QueuedIndex(String index, long timestamp) {
		this.index = index;
		this.timestamp = timestamp;
	}

	@Override
	public void writeMarshallable(BytesOut bytes) {
		bytes.writeUtf8(index);
		bytes.writeLong(timestamp);
	}

	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		index = bytes.readUtf8();
		timestamp = bytes.readLong();
	}

	public String getIndex() {
		return index;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof QueuedIndex)) return false;
		QueuedIndex that = (QueuedIndex) o;
		return Objects.equals(index, that.index);
	}

	@Override
	public int hashCode() {
		return Objects.hash(index);
	}

	@Override
	public int compareTo(QueuedIndex o) {
		return index.compareTo(o.index);
	}

	@Override
	public String getKey() {
		return index;
	}
}
