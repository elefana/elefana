/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
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
package com.elefana.node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class OsStats implements Runnable {
	private final com.elefana.es.compat.stats.OsStats stats = new com.elefana.es.compat.stats.OsStats();
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<String, Object> currentStats = new HashMap<String, Object>();

	@Override
	public void run() {
		lock.writeLock().lock();

		try {
			generateCurrentStats(currentStats);
		} catch (Exception e) {
		}

		lock.writeLock().unlock();
	}
	
	protected abstract void generateCurrentStats(Map<String, Object> result);

	public Map<String, Object> getCurrentStats() {
		lock.readLock().lock();
		Map<String, Object> result = new HashMap<String, Object>(currentStats);
		lock.readLock().unlock();
		return result;
	}
	
	public String getOsName() {
		return stats.getOsName();
	}
	
	public String getOsVersion() {
		return stats.getOsVersion();
	}
	
	public String getOsArch() {
		return stats.getOsArch();
	}
	
	public int getAvailableProcessors() {
		return stats.getAvailableProcessors();
	}

	public long getFreePhysicalMemorySize() {
		return stats.getFreePhysicalMemorySize();
	}

	public long getTotalPhysicalMemorySize() {
		return stats.getTotalPhysicalMemorySize();
	}

	public long getFreeSwapSpaceSize() {
		return stats.getFreeSwapSpaceSize();
	}

	public long getTotalSwapSpaceSize() {
		return stats.getTotalSwapSpaceSize();
	}

	public double[] getSystemLoadAverage() {
		return stats.getSystemLoadAverage();
	}

	public int getSystemCpuLoad() {
		return stats.getSystemCpuLoad();
	}
}
