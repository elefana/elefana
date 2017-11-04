/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.node;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mini2Dx.natives.OsInformation;

public abstract class OsStats implements Runnable {
	private static final OperatingSystemMXBean MX_BEAN = ManagementFactory.getOperatingSystemMXBean();

	private static final Method FREE_PHYSICAL_MEMORY_SIZE;
	private static final Method TOTAL_PHYSICAL_MEMORY_SIZE;
	private static final Method FREE_SWAP_SPACE_SIZE;
	private static final Method TOTAL_SWAP_SPACE_SIZE;
	private static final Method SYSTEM_LOAD_AVERAGE;
	private static final Method SYSTEM_CPU_LOAD;

	static {
		FREE_PHYSICAL_MEMORY_SIZE = getMxImplMethod("getFreePhysicalMemorySize");
		TOTAL_PHYSICAL_MEMORY_SIZE = getMxImplMethod("getTotalPhysicalMemorySize");
		FREE_SWAP_SPACE_SIZE = getMxImplMethod("getFreeSwapSpaceSize");
		TOTAL_SWAP_SPACE_SIZE = getMxImplMethod("getTotalSwapSpaceSize");
		SYSTEM_LOAD_AVERAGE = getMxImplMethod("getSystemLoadAverage");
		SYSTEM_CPU_LOAD = getMxImplMethod("getSystemCpuLoad");
	}

	private static Method getMxImplMethod(String methodName) {
		try {
			return Class.forName("com.sun.management.OperatingSystemMXBean").getMethod(methodName);
		} catch (Exception e) {
			// not available
			return null;
		}
	}

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

	public long getFreePhysicalMemorySize() {
		if (FREE_PHYSICAL_MEMORY_SIZE == null) {
			return -1;
		}
		try {
			return (long) FREE_PHYSICAL_MEMORY_SIZE.invoke(MX_BEAN);
		} catch (Exception e) {
			return -1;
		}
	}

	public long getTotalPhysicalMemorySize() {
		if (TOTAL_PHYSICAL_MEMORY_SIZE == null) {
			return -1;
		}
		try {
			return (long) TOTAL_PHYSICAL_MEMORY_SIZE.invoke(MX_BEAN);
		} catch (Exception e) {
			return -1;
		}
	}

	public long getFreeSwapSpaceSize() {
		if (FREE_SWAP_SPACE_SIZE == null) {
			return -1;
		}
		try {
			return (long) FREE_SWAP_SPACE_SIZE.invoke(MX_BEAN);
		} catch (Exception e) {
			return -1;
		}
	}

	public long getTotalSwapSpaceSize() {
		if (TOTAL_SWAP_SPACE_SIZE == null) {
			return -1;
		}
		try {
			return (long) TOTAL_SWAP_SPACE_SIZE.invoke(MX_BEAN);
		} catch (Exception e) {
			return -1;
		}
	}

	public double[] getSystemLoadAverage() {
		if (!OsInformation.isWindows()) {
			if (SYSTEM_LOAD_AVERAGE == null) {
				return null;
			}
			try {
				final double oneMinuteLoadAverage = (double) SYSTEM_LOAD_AVERAGE.invoke(MX_BEAN);
				return new double[] { oneMinuteLoadAverage >= 0 ? oneMinuteLoadAverage : -1, -1, -1 };
			} catch (Exception e) {
			}
		}
		return null;
	}

	public int getSystemCpuLoad() {
		if (SYSTEM_CPU_LOAD == null) {
			return -1;
		}
		try {
			double result = (double) SYSTEM_CPU_LOAD.invoke(MX_BEAN);
			if (result >= 0.0) {
				return (int) result * 100;
			}
		} catch (Exception e) {
		}
		return -1;
	}
}
