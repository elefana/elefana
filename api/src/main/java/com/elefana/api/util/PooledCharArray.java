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
package com.elefana.api.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PooledCharArray {
	private static final int MAX_POOL_SIZE = 1024 * 16;
	private static final Lock LOCK = new ReentrantLock();
	private static final List<char[]> POOL = new ArrayList<char[]>(MAX_POOL_SIZE);
	private static final AtomicInteger MAX_ARRAY_SIZE = new AtomicInteger(256);

	public static char[] allocate() {
		LOCK.lock();
		final char [] result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		if(result == null) {
			return new char[MAX_ARRAY_SIZE.get()];
		}
		return result;
	}

	public static char[] allocate(int length) {
		LOCK.lock();
		char [] result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		MAX_ARRAY_SIZE.set(Math.max(MAX_ARRAY_SIZE.get(), length));

		if(result == null) {
			return new char[MAX_ARRAY_SIZE.get()];
		}
		if(result.length < length) {
			result = new char[MAX_ARRAY_SIZE.get()];
		}
		return result;
	}

	public static void release(char [] arr) {
		MAX_ARRAY_SIZE.set(Math.max(MAX_ARRAY_SIZE.get(), arr.length));

		LOCK.lock();
		if(POOL.size() < MAX_POOL_SIZE) {
			POOL.add(arr);
		}
		LOCK.unlock();
	}
}
