package com.elefana.util;

import java.util.concurrent.atomic.AtomicInteger;

public class JsonCharArray extends ThreadLocal<char[]> {
	private static final AtomicInteger MAX_SIZE = new AtomicInteger(2048);

	@Override
	protected char[] initialValue() {
		return new char[MAX_SIZE.get()];
	}

	@Override
	public char[] get() {
		char [] result = super.get();
		if(result.length < MAX_SIZE.get()) {
			result = new char[MAX_SIZE.get()];
		}
		return result;
	}

	@Override
	public void set(char[] value) {
		MAX_SIZE.set(Math.max(MAX_SIZE.get(), value.length));
		super.set(value);
	}
}
