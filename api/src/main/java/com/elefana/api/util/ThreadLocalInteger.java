/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.api.util;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadLocalInteger {
	private final Map<Long, LocalInteger> integers = new ConcurrentHashMap<Long, LocalInteger>();
	private Callable<Integer> initialiser;

	public ThreadLocalInteger() {
		this.initialiser = new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				return 0;
			}
		};
	}

	public ThreadLocalInteger(Callable<Integer> initialiser) {
		this.initialiser = initialiser;
	}

	public int incrementAndGet() {
		return getLocalInteger().value++;
	}
	
	public String getThreadIdAndNextValue() {
		return Thread.currentThread().getId() + "" + incrementAndGet();
	}
	
	private LocalInteger getLocalInteger() {
		LocalInteger result = integers.get(Thread.currentThread().getId());
		if(result == null) {
			result = new LocalInteger();
			try {
				result.value = initialiser.call();
			} catch (Exception e) {}
			integers.put(Thread.currentThread().getId(), result);
		}
		return result;
	}

	private class LocalInteger {
		int value;
	}
}
