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

import java.util.Objects;

public class QueuedIndex implements Comparable<QueuedIndex> {
	private final String index;
	private final long timestamp;

	public QueuedIndex(String index, long timestamp) {
		this.index = index;
		this.timestamp = timestamp;
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
}
