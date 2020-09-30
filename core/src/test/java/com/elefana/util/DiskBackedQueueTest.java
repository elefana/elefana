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
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
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

		final TestData peekResult1 = new TestData();
		final TestData peekResult2 = new TestData();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		Assert.assertFalse(queue.peek(peekResult1));
		queue.offer(new TestData(expectedValue));

		final TestData pollResult = new TestData();

		Assert.assertTrue(queue.peek(peekResult1));
		Assert.assertTrue(queue.peek(peekResult2));
		queue.poll(pollResult);

		Assert.assertEquals(expectedValue, peekResult1.value);
		Assert.assertEquals(expectedValue, peekResult2.value);
		Assert.assertEquals(expectedValue, pollResult.value);

		Assert.assertFalse(queue.peek(peekResult2));
	}

	@Test
	public void testPeekOnRollover() throws Exception {
		final int expectedValue1 = 45867;
		final int expectedValue2 = 586;
		final int expectedValue3 = 586;

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final TestData peekResult1 = new TestData();
		final TestData peekResult2 = new TestData();
		final TestData peekResult3 = new TestData();

		SetTimeProvider timeProvider = new SetTimeProvider();
		timeProvider.currentTimeMillis(System.currentTimeMillis());

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_SECONDLY, timeProvider, false);
		Assert.assertFalse(queue.peek(peekResult1));
		queue.offer(new TestData(expectedValue1));
		queue.offer(new TestData(expectedValue2));

		final TestData pollResult = new TestData();

		timeProvider.advanceMillis(1500);
		queue.offer(new TestData(expectedValue3));

		final long prePeekIndex1 = queue.getTailerIndex();
		Assert.assertTrue(queue.peek(peekResult1));
		final long postPeekIndex1 = queue.getTailerIndex();
		Assert.assertEquals(expectedValue1, peekResult1.value);
		Assert.assertEquals(0, prePeekIndex1);
		Assert.assertNotEquals(0, postPeekIndex1);

		final long prePollIndex1 = queue.getTailerIndex();
		Assert.assertTrue(queue.poll(pollResult));
		final long postPollIndex1 = queue.getTailerIndex();
		Assert.assertEquals(prePollIndex1  + 1, postPollIndex1);
		Assert.assertEquals(expectedValue1, pollResult.value);

		final long prePeekIndex2 = queue.getTailerIndex();
		Assert.assertTrue(queue.peek(peekResult2));
		final long postPeekIndex2 = queue.getTailerIndex();
		Assert.assertEquals(prePeekIndex2, postPeekIndex2);
		Assert.assertEquals(expectedValue2, peekResult2.value);

		final long prePollIndex2 = queue.getTailerIndex();
		Assert.assertTrue(queue.poll(pollResult));
		final long postPollIndex2 = queue.getTailerIndex();
		Assert.assertNotEquals(prePollIndex2, postPollIndex2);
		Assert.assertEquals(expectedValue2, pollResult.value);

		final long prePeekIndex3 = queue.getTailerIndex();
		Assert.assertEquals(postPollIndex2, prePeekIndex3);
		Assert.assertTrue(queue.peek(peekResult3));
		final long postPeekIndex3 = queue.getTailerIndex();
		Assert.assertEquals(prePeekIndex3, postPeekIndex3);
		Assert.assertEquals(expectedValue3, peekResult3.value);

		final long prePollIndex3 = queue.getTailerIndex();
		Assert.assertTrue(queue.poll(pollResult));
		final long postPollIndex3 = queue.getTailerIndex();
		Assert.assertEquals(prePollIndex3 + 1, postPollIndex3);
		Assert.assertEquals(expectedValue3, pollResult.value);
	}

	@Test
	public void testPrune() throws Exception {
		final int expectedValue = 10385;

		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_SECONDLY);
		queue.offer(new TestData(expectedValue));

		for(int i = 0; i < 10; i++) {
			queue.prune();

			try {
				Thread.sleep(500);
			} catch (Exception e) {}
		}

		final TestData pollResult = new TestData();
		queue.poll(pollResult);
		Assert.assertEquals(expectedValue, pollResult.value);
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
		queue.dispose();

		final DiskBackedQueue<TestData> resumedQueue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST_DAILY);
		final TestData result = new TestData();
		for(int i = 0; i < totalItems; i++) {
			if(!resumedQueue.peek(result)) {
				Assert.fail("Expected data but there was none");
			}
			if(!resumedQueue.poll(result)) {
				Assert.fail("Expected data but there was none");
			}
			Assert.assertEquals(expectedQueue.get(i).intValue(), result.value);
		}

		resumedQueue.offer(new TestData(99));
		resumedQueue.poll(result);
		Assert.assertEquals(99, result.value);
	}

	@Test(timeout=10000L)
	public void testConcurrentOfferPoll() throws IOException {
		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final DiskBackedQueue<TestData> queue = new DiskBackedQueue<>(
				queueId, dataDirectory, TestData.class, RollCycles.TEST4_DAILY);

		final int totalThreads = 4;
		final CountDownLatch countDownLatch = new CountDownLatch(totalThreads);
		final Thread [] threads = new Thread[totalThreads];

		final Set<Integer> expected = new ConcurrentSkipListSet<>();
		final Set<Integer> result = new ConcurrentSkipListSet<>();

		for(int i = 0; i < totalThreads; i++) {
			if(i < totalThreads - 1) {
				final int threadIndex = i;
				threads[i] = new Thread(() -> {
					try {
						countDownLatch.countDown();
						countDownLatch.await();
					} catch (Exception e) {}

					final TestData resultData = new TestData();
					for(int j = threadIndex * 1000; j < (threadIndex * 1000) + 1000; j++) {
						queue.peek(resultData);
						Assert.assertTrue(queue.offer(new TestData(j)));
						expected.add(j);
					}
				});
			} else {
				threads[i] = new Thread(() -> {
					try {
						countDownLatch.countDown();
						countDownLatch.await();
					} catch (Exception e) {}

					final TestData resultData = new TestData();
					for(int j = 0; j < (totalThreads - 1) * 1000; j++) {
						if(j < ((totalThreads - 1) * 1000) - 1) {
							Assert.assertFalse(queueId.isEmpty());
						}
						if(!queue.peek(resultData)) {
							j--;
							continue;
						}
						if(queue.poll(resultData)) {
							result.add(resultData.value);
							continue;
						} else {
							j--;
						}
						queue.prune();
					}
				});
			}
		}

		for(int i = 0; i < totalThreads; i++) {
			threads[i].start();
		}

		for(int i = 0; i < totalThreads; i++) {
			try {
				threads[i].join();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Assert.assertEquals(expected.size(), result.size());

		for(int i : expected) {
			Assert.assertTrue(result.contains(i));
		}
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
