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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class OsStats implements Runnable {
	private final com.elefana.es.compat.stats.OsStats stats = new com.elefana.es.compat.stats.OsStats();

	private final Map<String, Object> tempStats = new HashMap<String, Object>();
	private final Map<String, Object> currentStats = new ConcurrentHashMap<>();

	private final AtomicReference<String> osName = new AtomicReference<>("");
	private final AtomicReference<String> osVersion = new AtomicReference<>("");
	private final AtomicReference<String> osArch = new AtomicReference<>("");
	private final AtomicInteger availableProcessors = new AtomicInteger(1);
	private final AtomicLong freePhysicalMemory = new AtomicLong(1);
	private final AtomicLong totalPhysicalMemory = new AtomicLong(1);
	private final AtomicLong freeSwapSpace = new AtomicLong(1);
	private final AtomicLong totalSwapSpace = new AtomicLong(1);
	private final AtomicReference<double[]> systemLoadAverage = new AtomicReference<>(new double[0]);
	private final AtomicInteger cpuLoad = new AtomicInteger(1);

	@Override
	public void run() {
		try {
			generateCurrentStats(tempStats);
			currentStats.putAll(tempStats);

			osName.set(stats.getOsName());
			osVersion.set(stats.getOsVersion());
			osArch.set(stats.getOsArch());
			availableProcessors.set(stats.getAvailableProcessors());
			freePhysicalMemory.set(stats.getFreePhysicalMemorySize());
			totalPhysicalMemory.set(stats.getTotalPhysicalMemorySize());
			freeSwapSpace.set(stats.getFreeSwapSpaceSize());
			totalSwapSpace.set(stats.getTotalSwapSpaceSize());
			systemLoadAverage.set(stats.getSystemLoadAverage());
			cpuLoad.set(stats.getSystemCpuLoad());
		} catch (Exception e) {
		}
	}
	
	protected abstract void generateCurrentStats(Map<String, Object> result);

	public Map<String, Object> getCurrentStats() {
		return currentStats;
	}
	
	public String getOsName() {
		return osName.get();
	}
	
	public String getOsVersion() {
		return osVersion.get();
	}
	
	public String getOsArch() {
		return osArch.get();
	}
	
	public int getAvailableProcessors() {
		return availableProcessors.get();
	}

	public long getFreePhysicalMemorySize() {
		return freePhysicalMemory.get();
	}

	public long getTotalPhysicalMemorySize() {
		return totalPhysicalMemory.get();
	}

	public long getFreeSwapSpaceSize() {
		return freeSwapSpace.get();
	}

	public long getTotalSwapSpaceSize() {
		return totalSwapSpace.get();
	}

	public double[] getSystemLoadAverage() {
		return systemLoadAverage.get();
	}

	public int getSystemCpuLoad() {
		return cpuLoad.get();
	}
}
