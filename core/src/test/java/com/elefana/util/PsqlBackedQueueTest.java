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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;

import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class PsqlBackedQueueTest{
	private static final long IO_INTERVAL = 1000L;
	private static final int HALF_CAPACITY = 5;
	private static final int MAX_CAPACITY = 10;

	private JdbcTemplate jdbcTemplate;
	private TaskScheduler taskScheduler;

	private TestQueue queue;

	private final AtomicInteger head = new AtomicInteger(0);
	private final AtomicInteger tail = new AtomicInteger(0);

	@Before
	public void setUp() throws SQLException {
		jdbcTemplate = mock(JdbcTemplate.class);
		taskScheduler = mock(TaskScheduler.class);

		when(taskScheduler.scheduleAtFixedRate(any(Runnable.class), eq(IO_INTERVAL))).thenReturn(mock(ScheduledFuture.class));
		when(taskScheduler.scheduleWithFixedDelay(any(Runnable.class), any(Long.class))).then(invocation -> {
			Runnable runnable = (Runnable) invocation.getArgument(0);
			runnable.run();
			return null;
		});

		queue = new TestQueue(jdbcTemplate, taskScheduler, IO_INTERVAL, MAX_CAPACITY);
	}

	@After
	public void teardown() {
		validateMockitoUsage();
	}

	@Test
	public void testQueueTransferInMemory() {
		final String expected = "Test 1";

		queue.offer(expected);
		queue.run();
		Assert.assertEquals(1, queue.size());
		Assert.assertEquals(expected, queue.peek());
		Assert.assertEquals(1, queue.size());
		Assert.assertEquals(expected, queue.poll());
		Assert.assertEquals(0, queue.size());
	}

	@Test
	public void testQueueTransferInMemoryDoubleRun() {
		final String expected = "Test 1";

		queue.offer(expected);
		queue.run();
		queue.run();
		Assert.assertEquals(1, queue.size());
		Assert.assertEquals(expected, queue.peek());
		Assert.assertEquals(1, queue.size());
		Assert.assertEquals(expected, queue.poll());
		Assert.assertEquals(0, queue.size());
	}

	@Test
	public void testQueueTransferViaDatabase() {
		push(MAX_CAPACITY);

		Assert.assertEquals(MAX_CAPACITY, queue.size());
		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.size());
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);
	}

	@Test
	public void testPollFromDatabase() {
		final String expected1 = "Test 1";
		final String expected2 = "Test 2";
		queue.offer(expected1);
		queue.addToDatabase(expected2);

		queue.debug();
		queue.run();
		queue.debug();

		Assert.assertEquals(2, queue.database.size());
		Assert.assertEquals(2, queue.memoryQueueSize());
		Assert.assertEquals(expected1, queue.poll());
		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(expected2, queue.poll());
		Assert.assertEquals(0, queue.size());
	}

	@Test
	public void testPollFromDatabaseOnly() {
		final String expected1 = "Test 1";
		final String expected2 = "Test 2";
		queue.addToDatabase(expected1);
		queue.addToDatabase(expected2);

		queue.debug();
		queue.run();
		queue.debug();

		Assert.assertEquals(2, queue.database.size());
		Assert.assertEquals(2, queue.memoryQueueSize());
		Assert.assertEquals(expected1, queue.poll());
		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(expected2, queue.poll());
		Assert.assertEquals(0, queue.size());
	}

	@Test
	public void testQueueTransferViaDatabasePushPullLoop() {
		for(int i = 0; i < 5; i++) {
			Assert.assertEquals((MAX_CAPACITY * i) - (i * HALF_CAPACITY), queue.size());
			push(MAX_CAPACITY);
			queue.run();
			pull(HALF_CAPACITY);
		}
		for(int i = 0; i < 5; i++) {
			Assert.assertEquals((MAX_CAPACITY * 5) - (5 * HALF_CAPACITY) - (i * HALF_CAPACITY), queue.size());
			queue.run();
			pull(HALF_CAPACITY);
		}
		queue.run();

		Assert.assertEquals(0, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(0, queue.getTotalDatabaseEntries());
		Assert.assertEquals(0, queue.size());
	}

	@Test
	public void testQueueTransferViaDatabasePushPullLoopOverCapacity() {
		for(int i = 0; i < 4; i++) {
			push(MAX_CAPACITY * 2);
			queue.run();
			pull(HALF_CAPACITY);
		}
		for(int i = 0; i < 12; i++) {
			queue.run();
			pull(HALF_CAPACITY);
		}
		queue.run();
	}

	@Test
	public void testOverThenUnderCapacity() {
		push(MAX_CAPACITY * 2);
		queue.run();
		pull(HALF_CAPACITY);
		queue.run();
		pull(HALF_CAPACITY);
		queue.run();
		pull(HALF_CAPACITY);
		queue.run();
		push(1);
		queue.run();
		pull(HALF_CAPACITY);
		pull(1);
	}

	@Test
	public void testQueueTransferViaDatabaseDoubleRun() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);
	}

	@Test
	public void testQueueTransferViaDatabaseSimpleOverCapacity() {
		push(MAX_CAPACITY * 2);

		Assert.assertEquals(10, queue.memoryQueueSize());
		Assert.assertEquals(8, queue.writeQueueSize());
		Assert.assertEquals(12, queue.getTotalDatabaseEntries());

		Assert.assertEquals(10, queue.databaseCursor());
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.databaseCursor());
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);
		Assert.assertEquals(MAX_CAPACITY, queue.databaseCursor());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.databaseCursor());

		pull(MAX_CAPACITY);
		Assert.assertEquals(MAX_CAPACITY, queue.databaseCursor());
		queue.run();
		Assert.assertEquals(0, queue.databaseCursor());
	}

	@Test
	public void testQueueTransferViaDatabaseSimpleOverCapacityDoubleRun() {
		for(int i = 0; i < MAX_CAPACITY * 2; i++) {
			queue.offer("Test " + i);
		}

		Assert.assertEquals(10, queue.memoryQueueSize());
		Assert.assertEquals(8, queue.writeQueueSize());
		Assert.assertEquals(12, queue.getTotalDatabaseEntries());

		queue.run();
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());
	}

	@Test
	public void testQueueTransferViaDatabaseSimplePushPull() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(HALF_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(0, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(0, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(0, queue.getTotalDatabaseEntries());
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueAtCapacity() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		push(MAX_CAPACITY);

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);

		Assert.assertEquals(0, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(5, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueOverCapacity() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		push(MAX_CAPACITY * 2);
		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY + (MAX_CAPACITY * 2), queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY + (MAX_CAPACITY * 2), queue.getTotalDatabaseEntries());

		queue.debug();
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.memoryQueueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		queue.debug();
		pull(MAX_CAPACITY);
	}

	@Test
	public void testQueueConcurrency() {
		final int totalThreads = 4;
		final Thread[] threads = new Thread[totalThreads];
		final CountDownLatch latch = new CountDownLatch(totalThreads);
		final Set<Integer> results = new ConcurrentSkipListSet<Integer>();

		for(int i = 0; i < totalThreads; i++) {
			if(i % 2 == 0) {
				threads[i] = new Thread(() -> {
					latch.countDown();
					push(MAX_CAPACITY);
				});
			} else {
				threads[i] = new Thread(() -> {
					latch.countDown();
					pull(results, HALF_CAPACITY);
				});
			}
		}

		for(int i = 0; i < totalThreads; i++) {
			threads[i].start();
		}
		for(int i = 0; i < totalThreads; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {}
		}

		queue.run();
		Assert.assertEquals(MAX_CAPACITY, results.size());
		Assert.assertEquals(MAX_CAPACITY, queue.database.size());
	}

	private void push(int entries) {
		for(int i = 0; i < entries; i++) {
			queue.offer(String.valueOf(tail.incrementAndGet()));
		}
	}

	private void pull(Set<Integer> results , int entries) {
		for(int i = 0; i < entries; i++) {
			while(queue.peek() == null) {
				try {
					Thread.sleep(10L);
				} catch (Exception e) {}
			}
			results.add(Integer.valueOf(queue.poll()));
		}
	}

	private void pull(int entries) {
		for(int i = 0; i < entries; i++) {
			while(queue.peek() == null) {
				try {
					Thread.sleep(10L);
				} catch (Exception e) {}
			}
			Assert.assertEquals(String.valueOf(head.incrementAndGet()), queue.poll());
		}
	}

	private class TestQueue extends PsqlBackedQueue<String> {
		private final CopyOnWriteArrayList<String> database = new CopyOnWriteArrayList<String>();

		public TestQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler, long ioIntervalMillis, int maxCapacity) throws SQLException {
			super(jdbcTemplate, taskScheduler, ioIntervalMillis, maxCapacity);
		}

		@Override
		public void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<String> results, int from, int limit) throws SQLException {
			if(database == null) {
				return;
			}
			System.out.println("DEBUG: Fetch from database. From: " + from + ", Limit: " + limit + ", Database Size: " + database.size());
			for(int i = from; i < from + limit && i < database.size(); i++) {
				results.add(database.get(i));
				System.out.println("DEBUG: Cursor " + i + " = Value " + database.get(i));
			}
		}

		@Override
		public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
			System.out.println("DEBUG: Remove " + size + " from database");
			for(int i = 0; i < size; i++) {
				System.out.println("DEBUG: Remove value " + database.remove(0));
			}
		}

		@Override
		public void appendToDatabase(JdbcTemplate jdbcTemplate, List<String> elements) throws SQLException {
			System.out.println("DEBUG: Append " + elements.size() + " elements to database");
			for(String element : elements) {
				database.add(element);
				System.out.println("DEBUG: Append into database value " + element + ", cursor " + databaseCursor + ", Database Size: " + database.size());
			}
		}

		@Override
		public int getDatabaseQueueSize(JdbcTemplate jdbcTemplate) throws SQLException {
			if(database == null) {
				return 0;
			}
			return database.size();
		}

		public boolean databaseContains(String element) {
			return database.contains(element);
		}

		public void addToDatabase(String element) {
			database.add(element);
			size.incrementAndGet();
		}

		public boolean isDatabaseEmpty() {
			return database.isEmpty();
		}

		public int getTotalDatabaseEntries() {
			return database.size();
		}

		public int databaseCursor() {
			return databaseCursor;
		}

		public void debug() {
			System.out.println("--------------");
			System.out.println("Database size: " + this.database.size());
			System.out.println("Queue size: " + this.queue.size());
			System.out.println("Write queue size: " + this.writeQueue.size());
			System.out.println("Database Cursor: " + databaseCursor);
			System.out.println("--------------");
		}
	}
}
