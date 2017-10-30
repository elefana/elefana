/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class ProcessStats implements Runnable {

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
}
