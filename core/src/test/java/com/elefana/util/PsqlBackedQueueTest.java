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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

public class PsqlBackedQueueTest{
	private static final long IO_INTERVAL = 1000L;
	private static final int HALF_CAPACITY = 5;
	private static final int MAX_CAPACITY = 10;

	private JdbcTemplate jdbcTemplate;
	private TaskScheduler taskScheduler;

	private TestQueue queue;

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
		Assert.assertEquals(expected, queue.peek());
		Assert.assertEquals(expected, queue.poll());
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueAtCapacity() {
		for(int i = 0; i < MAX_CAPACITY; i++) {
			queue.offer("Test " + i);
		}
		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		for(int i = 0; i < MAX_CAPACITY; i++) {
			queue.offer("Test " + (i + MAX_CAPACITY));
		}

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		for(int i = 0; i < HALF_CAPACITY; i++) {
			Assert.assertEquals("Test " + i, queue.poll());
		}

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());

		for(int i = 0; i < MAX_CAPACITY; i++) {
			Assert.assertEquals("Test " + (i + 5), queue.poll());
		}

		Assert.assertEquals(0, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(5, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		for(int i = 0; i < 5; i++) {
			Assert.assertEquals("Test " + (i + (MAX_CAPACITY + HALF_CAPACITY)), queue.poll());
		}
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueOverCapacity() {
		for(int i = 0; i < MAX_CAPACITY; i++) {
			queue.offer("Test " + i);
		}
		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		for(int i = 0; i < MAX_CAPACITY * 2; i++) {
			queue.offer("Test " + (i + MAX_CAPACITY));
		}
		for(int i = 0; i < HALF_CAPACITY; i++) {
			Assert.assertEquals("Test " + i, queue.poll());
		}

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY + (MAX_CAPACITY * 2), queue.getTotalDatabaseEntries());
	}


	private class TestQueue extends PsqlBackedQueue<String> {
		private final LinkedList<String> database = new LinkedList<String>();

		public TestQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler, long ioIntervalMillis, int maxCapacity) throws SQLException {
			super(jdbcTemplate, taskScheduler, ioIntervalMillis, maxCapacity);
		}

		@Override
		public void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<String> results, int from, int limit) throws SQLException {
			if(database == null) {
				return;
			}
			for(int i = from; i < from + limit && i < database.size(); i++) {
				results.add(database.get(i));
			}
			System.out.println("DEBUG: Fetch from database. From: " + from + ", Limit: " + limit);
		}

		@Override
		public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
			for(int i = 0; i < size; i++) {
				database.poll();
			}
			System.out.println("DEBUG: Remove " + size + " from database");
		}

		@Override
		public void appendToDatabase(JdbcTemplate jdbcTemplate, List<String> elements) throws SQLException {
			System.out.println("DEBUG: Append " + elements.size() + " to database");
			for(String element : elements) {
				database.offer(element);
			}
		}

		public boolean databaseContains(String element) {
			return database.contains(element);
		}

		public void addToDatabase(String element) {
			database.offer(element);
		}

		public boolean isDatabaseEmpty() {
			return database.isEmpty();
		}

		public int getTotalDatabaseEntries() {
			return database.size();
		}

		public int queueSize() {
			return queue.size();
		}

		public int writeQueueSize() {
			return writeQueue.size();
		}

		public void debug() {
			System.out.println("--------------");
			System.out.println("Queue size: " + this.queue.size());
			System.out.println("Write queue size: " + this.writeQueue.size());
			System.out.println("Database Cursor: " + databaseCursor);
			System.out.println("Write Queue Remainder: " + writeQueueRemainder);
			System.out.println("--------------");
		}
	}
}
