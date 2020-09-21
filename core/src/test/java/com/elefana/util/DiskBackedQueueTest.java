/*******************************************************************************
 * Copyright 2020 Viridian Software Limited
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
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.Assert;
import org.junit.Test;
import org.mini2Dx.natives.OsInformation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class DiskBackedQueueTest {

	@Test
	public void testClearImmediately() throws Exception {
		final int firstValue = 34634;

		final SetTimeProvider timeProvider = new SetTimeProvider();
		timeProvider.currentTimeMillis(1000);

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue1 = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST4_SECONDLY, timeProvider, false);
		queue1.offer(new TestData(firstValue));
		queue1.dispose();

		final DiskBackedQueue<TestData> queue2 = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST4_SECONDLY, timeProvider, true);
		final TestData testData = new TestData();
		Assert.assertFalse(queue2.poll(testData));
	}

	@Test
	public void testDeleteOldFiles() throws Exception {
		if(OsInformation.isWindows()) {
			return;
		}
		final SetTimeProvider timeProvider = new SetTimeProvider();
		timeProvider.currentTimeMillis(1000);

		final Random random = new Random(230584239);
		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST4_SECONDLY, timeProvider, false);
		final TestData testData = new TestData();
		for(int i = 0; i < 32; i++) {
			final int expectedResult = random.nextInt();
			queue.offer(new TestData(expectedResult));
			if(!queue.poll(testData)) {
				continue;
			}
			Assert.assertEquals(expectedResult, testData.value);
			timeProvider.advanceMillis(1000);
		}
		Assert.assertEquals(31, queue.prune());
	}

	@Test
	public void testPeek() throws Exception {
		final int expectedValue = 10385;

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		queue.offer(new TestData(expectedValue));

		final TestData peekResult1 = new TestData();
		final TestData peekResult2 = new TestData();
		final TestData pollResult = new TestData();

		queue.peek(peekResult1);
		queue.peek(peekResult2);
		queue.poll(pollResult);

		Assert.assertEquals(expectedValue, peekResult1.value);
		Assert.assertEquals(expectedValue, peekResult2.value);
		Assert.assertEquals(expectedValue, pollResult.value);

		Assert.assertFalse(queue.peek(peekResult2));
	}

	@Test
	public void testIsEmpty() throws Exception {
		final int expectedValue = 10385;

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		Assert.assertTrue(queue.isEmpty());

		queue.offer(new TestData(expectedValue));
		Assert.assertFalse(queue.isEmpty());

		final TestData peekResult1 = new TestData();
		final TestData peekResult2 = new TestData();
		final TestData pollResult = new TestData();

		queue.peek(peekResult1);
		Assert.assertFalse(queue.isEmpty());
		queue.peek(peekResult2);
		Assert.assertFalse(queue.isEmpty());
		queue.poll(pollResult);
		Assert.assertTrue(queue.isEmpty());

		Assert.assertEquals(expectedValue, peekResult1.value);
		Assert.assertEquals(expectedValue, peekResult2.value);
		Assert.assertEquals(expectedValue, pollResult.value);

		Assert.assertFalse(queue.peek(peekResult2));
		Assert.assertTrue(queue.isEmpty());
	}

	@Test
	public void testResume() throws Exception {
		final int totalItems = 32;
		final Random random = new Random(123456);
		final List<Integer> expectedQueue = new ArrayList<Integer>();
		for(int i = 0; i < totalItems; i++) {
			expectedQueue.add(random.nextInt());
		}

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		for(int i = 0; i < totalItems; i++) {
			queue.offer(new TestData(expectedQueue.get(i)));
		}
		Assert.assertFalse(queue.isEmpty());
		queue.dispose();

		final DiskBackedQueue<TestData> resumedQueue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		Assert.assertFalse(resumedQueue.isEmpty());
		final TestData result = new TestData();
		for(int i = 0; i < totalItems; i++) {
			if(!resumedQueue.poll(result)) {
				Assert.fail("Expected data but there was none");
			}
			Assert.assertEquals(expectedQueue.get(i).intValue(), result.value);
		}
	}

	@Test
	public void testConcurrentOfferPoll() throws IOException {
		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST4_DAILY);

		final CountDownLatch countDownLatch = new CountDownLatch(2);
		final Thread [] threads = new Thread[2];
		threads[0] = new Thread(() -> {
			try {
				countDownLatch.countDown();
				countDownLatch.await();
			} catch (Exception e) {}

			for(int i = 0; i < 1000; i++) {
				queue.offer(new TestData(i));
			}
		});
		threads[1] = new Thread(() -> {
			try {
				countDownLatch.countDown();
				countDownLatch.await();
			} catch (Exception e) {}

			final TestData result = new TestData();
			for(int i = 0; i < 1000; i++) {
				if(queue.poll(result)) {
					Assert.assertEquals(i, result.value);
				} else {
					i--;
				}
				queue.prune();
			}
		});

		try {
			threads[0].start();
			threads[1].start();
			threads[0].join();
			threads[1].join();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Assert.assertEquals(true, queue.isEmpty());
	}

	public static class TestData implements BytesMarshallable {
		private static final int MASHALLABLE_VERSION = 1;

		public int value;

		public TestData() {}

		public TestData(int value) {
			this.value = value;
		}

		@Override
		public void writeMarshallable(BytesOut bytes) {
			bytes.writeStopBit(MASHALLABLE_VERSION);
			bytes.writeInt(value);
		}

		@Override
		public void readMarshallable(BytesIn bytes) throws IORuntimeException {
			long stopBit = bytes.readStopBit();
			if(stopBit != MASHALLABLE_VERSION) {
				throw new IllegalStateException("Expected stop bit to be " + MASHALLABLE_VERSION + " but was " + stopBit);
			}
			value = bytes.readInt();
		}
	}
}
