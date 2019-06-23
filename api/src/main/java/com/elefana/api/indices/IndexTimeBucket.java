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
package com.elefana.api.indices;

public enum IndexTimeBucket {
	MINUTE(1000 * 60, 1000, 60),
	HOUR(1000 * 60 * 60, 1000 * 60, 60),
	DAY(1000 * 60 * 60 * 24, 1000 * 60 * 60, 24);

	private final long bucketOperand, offsetOperand;
	private final int ingestTableCapacity;

	IndexTimeBucket(long bucketOperand, long offsetOperand, int ingestTableCapacity) {
		this.bucketOperand = bucketOperand;
		this.offsetOperand = offsetOperand;
		this.ingestTableCapacity = ingestTableCapacity;
	}

	public int getIngestTableCapacity() {
		return ingestTableCapacity;
	}

	public int getShardOffset(long timestamp) {
		return (int) ((timestamp % bucketOperand) / offsetOperand);
	}

	public long getBucketOperand() {
		return bucketOperand;
	}

	public long getBucketInterval() {
		return offsetOperand;
	}
}
