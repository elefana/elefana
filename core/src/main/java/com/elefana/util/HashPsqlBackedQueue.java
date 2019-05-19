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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class HashPsqlBackedQueue<T> extends PsqlBackedQueue<T> {
	private Set<T> uniqueElements;
	private List<T> tmpElements;

	public HashPsqlBackedQueue(JdbcTemplate jdbcTemplate, TaskScheduler taskScheduler,
	                           long ioIntervalMillis, int maxCapacity) throws SQLException {
		super(jdbcTemplate, taskScheduler, ioIntervalMillis, maxCapacity);

	}

	protected abstract void fetchFromDatabaseUnique(JdbcTemplate jdbcTemplate, List<T> results, int from, int limit) throws SQLException;

	@Override
	public void fetchFromDatabase(JdbcTemplate jdbcTemplate, List<T> results, int from, int limit) throws SQLException {
		if(tmpElements == null) {
			tmpElements = new ArrayList<T>(maxCapacity);
		}
		if(uniqueElements == null) {
			uniqueElements = new ConcurrentSkipListSet<T>();
		}

		tmpElements.clear();
		fetchFromDatabaseUnique(jdbcTemplate, tmpElements, from, limit);

		for(T element : tmpElements) {
			if(uniqueElements.add(element)) {
				results.add(element);
			}
 		}
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		if(uniqueElements.addAll(c)) {
			return super.addAll(c);
		}
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		if(uniqueElements.retainAll(c)) {
			return super.removeAll(c);
		}
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		if(uniqueElements.retainAll(c)) {
			return super.retainAll(c);
		}
		return false;
	}

	@Override
	public boolean offer(T t) {
		if(uniqueElements.add(t)) {
			return super.offer(t);
		}
		return false;
	}

	@Override
	public T poll() {
		final T result = super.poll();
		if(result != null) {
			uniqueElements.remove(result);
		}
		return result;
	}

	@Override
	public void clear() {
		uniqueElements.clear();
		super.clear();
	}
}
