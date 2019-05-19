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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class PsqlBackedQueue<T> implements Queue<T>, Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlBackedQueue.class);

	protected final int maxCapacity;
	protected final ReadWriteLock lock = new ReentrantReadWriteLock();

	protected final List<T> queue;
	protected final List<T> writeQueue;

	private final JdbcTemplate jdbcTemplate;
	private final TaskScheduler taskScheduler;
	private final AtomicBoolean dirty = new AtomicBoolean();

	protected int previousQueueSize = 0;
	protected int databaseCursor = 0;
	protected boolean writeQueueInMemory = true;

	public PsqlBackedQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler,
	                       long ioIntervalMillis, int maxCapacity)
			throws SQLException {
		super();
		this.maxCapacity = maxCapacity;
		this.jdbcTemplate = jdbcTemplate;
		this.taskScheduler = taskScheduler;

		queue = new ArrayList<T>(maxCapacity + 2);
		writeQueue = new ArrayList<T>(maxCapacity + 2);

		fetchFromDatabase(0);

		taskScheduler.scheduleAtFixedRate(this, ioIntervalMillis);
	}

	public abstract void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<T> results, int from, int limit) throws SQLException;

	public abstract void removeFromDatabase(JdbcTemplate jdbcTemplate, int size) throws SQLException;

	public abstract void appendToDatabase(JdbcTemplate jdbcTemplate, List<T> elements) throws SQLException;

	private void fetchFromDatabase(int from) throws SQLException {
		int previousSize = queue.size();
		fetchFromDatabase(jdbcTemplate, queue, from, maxCapacity - previousSize);
		databaseCursor += queue.size() - previousSize;
		previousQueueSize = queue.size();
	}

	private void syncToDatabase() throws SQLException {
		if(queue.size() < previousQueueSize) {
			int delta = previousQueueSize - queue.size();
			removeFromDatabase(jdbcTemplate, delta);
			databaseCursor -= delta;
		}

		if(writeQueue.size() > 0) {
			appendToDatabase(jdbcTemplate, writeQueue);
		}

		if(queue.size() < maxCapacity) {
			if(!writeQueueInMemory || writeQueue.isEmpty()) {
				//Next entries need to come from database
				fetchFromDatabase(databaseCursor);
				writeQueueInMemory = false;
			} else {
				//Entries are already in memory
				while(queue.size() < maxCapacity) {
					if(writeQueue.isEmpty()) {
						break;
					}
					queue.add(writeQueue.remove(0));
					databaseCursor++;
				}

				writeQueueInMemory &= writeQueue.isEmpty();
			}
		} else if(writeQueue.size() > 0) {
			writeQueueInMemory = false;
		}
		writeQueue.clear();
		previousQueueSize = queue.size();
	}

	@Override
	public void run() {
		if(!dirty.get()) {
			return;
		}
		lock.writeLock().lock();

		try {
			syncToDatabase();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		dirty.set(false);
		lock.writeLock().unlock();
	}

	@Override
	public boolean contains(Object o) {
		lock.readLock().lock();
		final boolean result = queue.contains(o);
		lock.readLock().unlock();
		return result;
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		lock.readLock().lock();
		final Object [] result = queue.toArray();
		lock.readLock().unlock();
		return result;
	}

	@Override
	public <T1> T1[] toArray(T1[] a) {
		lock.readLock().lock();
		final T1 [] result = queue.toArray(a);
		lock.readLock().unlock();
		return result;
	}

	@Override
	public boolean add(T t) {
		return offer(t);
	}

	@Override
	public boolean remove(Object o) {
		lock.writeLock().lock();
		final boolean result = queue.remove(o);
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
		}
		return result;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		lock.readLock().lock();
		final boolean result = queue.containsAll(c);
		lock.readLock().unlock();
		return result;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		lock.writeLock().lock();
		final int previousQueueSize = queue.size();
		final boolean result = writeQueue.addAll(c);
		final int writeQueueSize = writeQueue.size();
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
		}
		if(previousQueueSize == 0 || writeQueueSize > maxCapacity) {
			taskScheduler.scheduleWithFixedDelay(this, 10L);
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		lock.writeLock().lock();
		final boolean result = queue.removeAll(c);
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		lock.writeLock().lock();
		final boolean result = queue.retainAll(c);
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
		}
		return result;
	}

	@Override
	public void clear() {
		lock.writeLock().lock();
		boolean result = !queue.isEmpty();
		queue.clear();
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
		}
	}

	@Override
	public boolean offer(T t) {
		lock.writeLock().lock();
		final int previousQueueSize = queue.size();
		final boolean result = writeQueue.add(t);
		final int writeQueueSize = writeQueue.size();
		lock.writeLock().unlock();

		if(result) {
			dirty.set(true);
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
		lock.writeLock().lock();
		final T result;
		if(queue.isEmpty()) {
			result = null;
		} else {
			result = queue.remove(0);
		}
		lock.writeLock().unlock();

		if(result != null) {
			dirty.set(true);
		}
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
		lock.readLock().lock();
		final T result;
		if(queue.isEmpty()) {
			result = null;
		} else {
			result = queue.get(0);
		}
		lock.readLock().unlock();
		return result;
	}

	@Override
	public int size() {
		lock.readLock().lock();
		final int result = queue.size();
		lock.readLock().unlock();
		return result;
	}

	@Override
	public boolean isEmpty() {
		lock.readLock().lock();
		final boolean result = queue.isEmpty();
		lock.readLock().unlock();
		return result;
	}
}
