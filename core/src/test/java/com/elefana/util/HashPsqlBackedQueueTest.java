package com.elefana.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class HashPsqlBackedQueueTest {
	private static final long IO_INTERVAL = 1000L;
	private static final int HALF_CAPACITY = 5;
	private static final int MAX_CAPACITY = 10;

	private JdbcTemplate jdbcTemplate;
	private TaskScheduler taskScheduler;

	private TestQueue queue;

	private int head = 0;
	private int tail = 0;

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
	public void testQueueTransferInMemoryDoubleRun() {
		final String expected = "Test 1";

		queue.offer(expected);
		queue.run();
		queue.run();
		Assert.assertEquals(expected, queue.peek());
		Assert.assertEquals(expected, queue.poll());
	}

	@Test
	public void testQueueTransferViaDatabase() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);
	}

	@Test
	public void testQueueTransferViaDatabasePushPullLoop() {
		for(int i = 0; i < 5; i++) {
			push(MAX_CAPACITY);
			queue.run();
			pull(HALF_CAPACITY);
		}
		for(int i = 0; i < 5; i++) {
			queue.run();
			pull(HALF_CAPACITY);
		}
		queue.run();

		Assert.assertEquals(0, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(0, queue.getTotalDatabaseEntries());
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

		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);
	}

	@Test
	public void testQueueTransferViaDatabaseSimpleOverCapacity() {
		push(MAX_CAPACITY * 2);

		Assert.assertEquals(10, queue.queueSize());
		Assert.assertEquals(8, queue.writeQueueSize());
		Assert.assertEquals(12, queue.getTotalDatabaseEntries());

		Assert.assertEquals(10, queue.databaseCursor());
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.databaseCursor());
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
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

		Assert.assertEquals(10, queue.queueSize());
		Assert.assertEquals(8, queue.writeQueueSize());
		Assert.assertEquals(12, queue.getTotalDatabaseEntries());

		queue.run();
		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());
	}

	@Test
	public void testQueueTransferViaDatabaseSimplePushPull() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(0, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(0, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(0, queue.getTotalDatabaseEntries());
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueAtCapacity() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(false, queue.isDatabaseEmpty());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		push(MAX_CAPACITY);

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(MAX_CAPACITY);

		Assert.assertEquals(0, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY + HALF_CAPACITY, queue.getTotalDatabaseEntries());
		queue.run();
		Assert.assertEquals(5, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY, queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);
	}

	@Test
	public void testQueueTransferViaDatabaseWriteQueueOverCapacity() {
		push(MAX_CAPACITY);

		Assert.assertEquals(1, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		Assert.assertEquals(1, queue.getTotalDatabaseEntries());

		queue.run();

		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY, queue.getTotalDatabaseEntries());

		push(MAX_CAPACITY * 2);
		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(9, queue.writeQueueSize());
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY + (MAX_CAPACITY * 2), queue.getTotalDatabaseEntries());

		pull(HALF_CAPACITY);

		Assert.assertEquals(HALF_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(HALF_CAPACITY + (MAX_CAPACITY * 2), queue.getTotalDatabaseEntries());

		queue.debug();
		queue.run();
		Assert.assertEquals(MAX_CAPACITY, queue.queueSize());
		Assert.assertEquals(0, queue.writeQueueSize());
		Assert.assertEquals(MAX_CAPACITY * 2, queue.getTotalDatabaseEntries());

		queue.debug();
		pull(MAX_CAPACITY);
	}

	private void push(int entries) {
		for(int i = 0; i < entries; i++) {
			queue.offer(String.valueOf(tail));
			tail++;
		}
	}

	private void pull(int entries) {
		for(int i = 0; i < entries; i++) {
			Assert.assertEquals(String.valueOf(head), queue.poll());
			head++;
		}
	}

	private class TestQueue extends HashPsqlBackedQueue<String> {
		private final LinkedList<String> database = new LinkedList<String>();

		public TestQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler, long ioIntervalMillis, int maxCapacity) throws SQLException {
			super(jdbcTemplate, taskScheduler, ioIntervalMillis, maxCapacity);
		}

		@Override
		protected void fetchFromDatabaseUnique(JdbcTemplate jdbcTemplate, List<String> results, int from, int limit) throws SQLException {
			if(database == null) {
				return;
			}
			System.out.println("DEBUG: Fetch from database. From: " + from + ", Limit: " + limit);
			for(int i = from; i < from + limit && i < database.size(); i++) {
				results.add(database.get(i));
				System.out.println("DEBUG: Fetch into queue cursor " + i + " = " + database.get(i));
			}
		}

		@Override
		public void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException {
			System.out.println("DEBUG: Remove " + size + " from database");
			for(int i = 0; i < size; i++) {
				System.out.println("DEBUG: Remove " + database.poll());
			}
		}

		@Override
		public int getDatabaseQueueSize(JdbcTemplate jdbcTemplate) throws SQLException {
			if(database == null) {
				return 0;
			}
			return database.size();
		}

		@Override
		public void appendToDatabaseUnique(JdbcTemplate jdbcTemplate, List<String> elements) throws SQLException {
			System.out.println("DEBUG: Append " + elements.size() + " to database");
			for(String element : elements) {
				database.offer(element);
				System.out.println("DEBUG: Append into database " + element + " " + databaseCursor);
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
