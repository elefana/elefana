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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class PsqlBackedQueue<T> implements Queue<T>, Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBackedQueue.class);

	protected final int maxCapacity;
	protected final long ioIntervalMillis;
	protected final ReadWriteLock queueLock = new ReentrantReadWriteLock();
	protected final ReadWriteLock writeQueueLock = new ReentrantReadWriteLock();
	protected final Lock flushLock = new ReentrantLock();

	protected final List<T> queue;
	protected final List<T> writeQueue;
	protected final List<T> flushQueue;

	private final JdbcTemplate jdbcTemplate;
	private final TaskScheduler taskScheduler;

	protected final AtomicInteger size = new AtomicInteger();

	protected int previousQueueSize = 0;
	protected int databaseCursor = 0;

	public PsqlBackedQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler,
	                       long ioIntervalMillis, int maxCapacity)
			throws SQLException {
		super();
		this.maxCapacity = maxCapacity;
		this.ioIntervalMillis = ioIntervalMillis;
		this.jdbcTemplate = jdbcTemplate;
		this.taskScheduler = taskScheduler;

		queue = new ArrayList<T>(maxCapacity + 2);
		writeQueue = new ArrayList<T>(maxCapacity + 2);
		flushQueue = new ArrayList<T>(maxCapacity + 2);

		try {
			fetchFromDatabase(0);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		try {
			size.set(getDatabaseQueueSize(jdbcTemplate));
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}

		taskScheduler.scheduleAtFixedRate(this, ioIntervalMillis);
	}

	public abstract void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<T> results, int from, int limit) throws SQLException;

	public abstract void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException;

	public abstract void appendToDatabase(JdbcTemplate jdbcTemplate, List<T> elements) throws SQLException;

	public abstract int getDatabaseQueueSize(JdbcTemplate jdbcTemplate) throws SQLException;

	protected void transferInMemory(T element) {
		queue.add(element);
	}

	private void fetchFromDatabase(int from) throws SQLException {
		try {
			int previousSize = queue.size();
			fetchFromDatabase(jdbcTemplate, queue, from, maxCapacity - previousSize);
			databaseCursor += queue.size() - previousSize;
			previousQueueSize = queue.size();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void syncToDatabase() throws SQLException {
		queueLock.readLock().lock();
		int currentQueueSize = queue.size();
		queueLock.readLock().unlock();

		if(!flushLock.tryLock()) {
			taskScheduler.scheduleWithFixedDelay(this, Math.max(10L, ioIntervalMillis / 10));
			return;
		}

		if(currentQueueSize < previousQueueSize) {
			int delta = previousQueueSize - currentQueueSize;
			try {
				removeFromDatabase(jdbcTemplate, delta);
				databaseCursor -= delta;
			} catch (SQLException e) {
				flushLock.unlock();
				throw e;
			}
		}

		writeQueueLock.writeLock().lock();
		flushQueue.clear();
		flushQueue.addAll(writeQueue);
		writeQueue.clear();
		writeQueueLock.writeLock().unlock();

		if(flushQueue.size() > 0) {
			try {
				appendToDatabase(jdbcTemplate, flushQueue);
			} catch (SQLException e) {
				flushLock.unlock();
				throw e;
			}
		}

		while(currentQueueSize < maxCapacity) {
			final int startQueueSize = currentQueueSize;
			if(flushQueue.size() > 0 && size.get() - flushQueue.size() < maxCapacity) {
				//Next queued items are in memory
				queueLock.writeLock().lock();
				transferInMemory(flushQueue.remove(0));
				currentQueueSize = queue.size();
				queueLock.writeLock().unlock();

				databaseCursor++;
			} else {
				//Next queued items are in database
				queueLock.writeLock().lock();
				fetchFromDatabase(databaseCursor);
				currentQueueSize = queue.size();
				queueLock.writeLock().unlock();
			}
			if(startQueueSize == currentQueueSize) {
				break;
			}
		}

		flushQueue.clear();
		previousQueueSize = currentQueueSize;
		flushLock.unlock();
	}

	@Override
	public void run() {
		try {
			syncToDatabase();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	public boolean contains(Object o) {
		queueLock.readLock().lock();
		final boolean result = queue.contains(o);
		queueLock.readLock().unlock();
		return result;
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		queueLock.readLock().lock();
		final Object [] result = queue.toArray();
		queueLock.readLock().unlock();
		return result;
	}

	@Override
	public <T1> T1[] toArray(T1[] a) {
		queueLock.readLock().lock();
		final T1 [] result = queue.toArray(a);
		queueLock.readLock().unlock();
		return result;
	}

	@Override
	public boolean add(T t) {
		return offer(t);
	}

	@Override
	public boolean remove(Object o) {
		queueLock.writeLock().lock();
		final boolean result = queue.remove(o);
		queueLock.writeLock().unlock();

		if(result) {
			size.decrementAndGet();
		}
		return result;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		queueLock.readLock().lock();
		final boolean result = queue.containsAll(c);
		queueLock.readLock().unlock();
		return result;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		queueLock.readLock().lock();
		final int previousQueueSize = queue.size();
		queueLock.readLock().unlock();

		writeQueueLock.writeLock().lock();
		final boolean result = writeQueue.addAll(c);
		final int writeQueueSize = writeQueue.size();
		writeQueueLock.writeLock().unlock();

		if(result) {
			size.addAndGet(c.size());
		}
		if(previousQueueSize == 0 || writeQueueSize > maxCapacity) {
			taskScheduler.scheduleWithFixedDelay(this, 10L);
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		queueLock.writeLock().lock();
		final int previousSize = queue.size();
		final boolean result = queue.removeAll(c);
		queueLock.writeLock().unlock();

		if(result) {
			size.getAndAdd(queue.size() - previousSize);
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		queueLock.writeLock().lock();
		final int previousSize = queue.size();
		final boolean result = queue.retainAll(c);
		size.getAndAdd(queue.size() - previousSize);
		queueLock.writeLock().unlock();
		return result;
	}

	@Override
	public void clear() {
		queueLock.writeLock().lock();
		boolean result = !queue.isEmpty();
		queue.clear();
		size.set(0);
		queueLock.writeLock().unlock();
	}

	@Override
	public boolean offer(T t) {
		writeQueueLock.writeLock().lock();
		final int previousQueueSize = queue.size();
		final boolean result = writeQueue.add(t);
		final int writeQueueSize = writeQueue.size();
		writeQueueLock.writeLock().unlock();

		if(result) {
			size.incrementAndGet();
		}
		if(previousQueueSize == 0 || writeQueueSize > maxCapacity) {
			taskScheduler.scheduleWithFixedDelay(this, 10L);
		}
		return result;
	}

	@Override
	public T remove() {
		final T result = poll();
		if(result == null) {
			throw new NoSuchElementException();
		}
		return result;
	}

	@Override
	public T poll() {
		queueLock.writeLock().lock();
		final T result;
		if(queue.isEmpty()) {
			result = null;
		} else {
			result = queue.remove(0);
			size.decrementAndGet();
		}
		queueLock.writeLock().unlock();
		return result;
	}

	@Override
	public T element() {
		final T result = peek();
		if(result == null) {
			throw new NoSuchElementException();
		}
		return result;
	}

	@Override
	public T peek() {
		queueLock.readLock().lock();
		final T result;
		if(queue.isEmpty()) {
			result = null;
		} else {
			result = queue.get(0);
		}
		queueLock.readLock().unlock();
		return result;
	}

	@Override
	public int size() {
		return size.get();
	}

	@Override
	public boolean isEmpty() {
		return size.get() == 0L;
	}

	public int memoryQueueSize() {
		queueLock.readLock().lock();
		final int result = queue.size();
		queueLock.readLock().unlock();
		return result;
	}

	public int writeQueueSize() {
		writeQueueLock.readLock().lock();
		final int result = writeQueue.size();
		writeQueueLock.readLock().unlock();
		return result;
	}
}
