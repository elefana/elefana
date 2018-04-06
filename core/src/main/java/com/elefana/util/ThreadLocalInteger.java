/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadLocalInteger {
	private final Map<Long, LocalInteger> integers = new ConcurrentHashMap<Long, LocalInteger>();
	
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
			integers.put(Thread.currentThread().getId(), result);
		}
		return result;
	}

	private class LocalInteger {
		int value;
	}
}
