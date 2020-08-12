/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class HashDiskBackedQueueTest {

	@Test
	public void testConcurrentOffer() throws IOException {
		final String queueId = UUID.randomUUID().toString();
		final File dataDirectory = Files.createTempDirectory(queueId).toFile();

		final HashDiskBackedQueue<TestData> queue = new HashDiskBackedQueue<TestData>(
				queueId, dataDirectory, TestData.class, 1000, String.valueOf(Integer.MAX_VALUE), new TestData(10),
				RollCycles.TEST_DAILY, false);

		final int totalThreads = 4;
		final Thread [] threads = new Thread[totalThreads];
		final CountDownLatch countDownLatch = new CountDownLatch(totalThreads);

		final int value = 1001;

		for(int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(() -> {
				try {
					countDownLatch.countDown();
					countDownLatch.await();
				} catch (Exception e) {}

				queue.offer(new TestData(value));
			});
		}
		for(int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for(int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {}
		}

		final TestData result = new TestData();
		Assert.assertTrue(queue.poll(result));
		Assert.assertEquals(value, result.value);

		Assert.assertFalse(queue.poll(result));
	}

	public static class TestData extends UniqueSelfDescribingMarshallable {
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

		@Override
		public String getKey() {
			return String.valueOf(value);
		}
	}
}
