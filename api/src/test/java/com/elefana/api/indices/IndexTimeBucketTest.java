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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class IndexTimeBucketTest {

	@Test
	public void testMinuteTimeBucket() {
		final long timestamp = 1554647400000L;
		for(long l = 0; l < (2 * 60 * 1000); l += 5) {
			final int expectedOffset = (int) (l >= 60000 ? (l - 60000) / 1000 : l / 1000);
			Assert.assertEquals(expectedOffset, IndexTimeBucket.MINUTE.getShardOffset(timestamp + l));
		}
		Assert.assertEquals(0, IndexTimeBucket.MINUTE.getShardOffset(timestamp));
		Assert.assertEquals(59, IndexTimeBucket.MINUTE.getShardOffset(timestamp - 1L));
	}

	@Test
	public void testHourTimeBucket() {
		final long timestamp = 1554645600000L;
		for(long l = 0; l < (2 * 60 * 60 * 1000); l += (30 * 1000)) {
			final int expectedOffset = (int) (l >= TimeUnit.HOURS.toMillis(1) ? (l - TimeUnit.HOURS.toMillis(1)) / (1000 * 60) : l / (1000 * 60));
			Assert.assertEquals(expectedOffset, IndexTimeBucket.HOUR.getShardOffset(timestamp + l));
		}
		Assert.assertEquals(0, IndexTimeBucket.HOUR.getShardOffset(timestamp));
		Assert.assertEquals(59, IndexTimeBucket.HOUR.getShardOffset(timestamp - 1L));
	}

	@Test
	public void testDayTimeBucket() {
		final long timestamp = 1554595200000L;
		for(long l = 0; l < (48 * 60 * 60 * 1000); l += (30 * 60 * 1000)) {
			final int expectedOffset = (int) (l >= TimeUnit.DAYS.toMillis(1) ? (l - TimeUnit.DAYS.toMillis(1)) / (1000 * 60 * 60) : l / (1000 * 60 * 60));
			Assert.assertEquals(expectedOffset, IndexTimeBucket.DAY.getShardOffset(timestamp + l));
		}
		Assert.assertEquals(0, IndexTimeBucket.DAY.getShardOffset(timestamp));
		Assert.assertEquals(23, IndexTimeBucket.DAY.getShardOffset(timestamp - 1L));
	}
}
