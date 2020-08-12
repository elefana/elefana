/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.queue.RollCycles;

import java.io.File;

public class HashDiskBackedQueue<T extends UniqueSelfDescribingMarshallable> extends DiskBackedQueue<T> {
	private final DiskBackedMap<String, T> backedMap;

	public HashDiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz,
	                           int expectedEntries, String averageKey, T averageValue) {
		this(queueId, dataDirectory, clazz, expectedEntries, averageKey, averageValue, false);
	}

	public HashDiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz,
							   int expectedEntries, String averageKey, T averageValue,
							   boolean cleanImmediately) {
		this(queueId, dataDirectory, clazz, expectedEntries, averageKey, averageValue, RollCycles.DAILY, cleanImmediately);
	}


	public HashDiskBackedQueue(String queueId, File dataDirectory, Class<T> clazz,
	                           int expectedEntries, String averageKey, T averageValue,
	                           RollCycles rollCycles,  boolean cleanImmediately) {
		super(queueId, dataDirectory, clazz, rollCycles, cleanImmediately);
		backedMap = new DiskBackedMap<String, T>(queueId, String.class, clazz,
				dataDirectory, expectedEntries, averageKey, averageValue, cleanImmediately);
	}


	@Override
	public void dispose() {
		backedMap.dispose();
		super.dispose();
	}

	@Override
	public void clear() {
		super.clear();
		backedMap.clear();
	}

	@Override
	public boolean poll(T result) {
		final boolean success = super.poll(result);
		if(success) {
			backedMap.remove(result.getKey());
		}
		return success;
	}

	@Override
	public boolean offer(T t) {
		T result = backedMap.computeIfAbsent(t.getKey(), (k) -> {
			final boolean success = super.offer(t);
			if(success) {
				return t;
			}
			return null;
		});
		return result != null;
	}
}
