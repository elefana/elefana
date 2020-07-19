/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class NoAllocTimestampExtractor implements NoAllocJsonReader.JsonReaderListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(NoAllocTimestampExtractor.class);

	private final NoAllocJsonReader jsonReader = new NoAllocJsonReader();
	private final char [] key;
	private final char [] temp;

	private boolean foundTimestamp;

	private long result = -1;

	public NoAllocTimestampExtractor(String key) {
		this.key = key.toCharArray();
		temp = new char[this.key.length];
	}

	public long extract(PooledStringBuilder str) {
		jsonReader.read(str, this);
		if(result == -1L) {
			return System.currentTimeMillis();
		}
		return result;
	}

	@Override
	public boolean onReadBegin() {
		foundTimestamp = false;
		result = -1L;
		return true;
	}

	@Override
	public boolean onKey(char[] value, int from, int length) {
		if(length != key.length) {
			return true;
		}
		System.arraycopy(value, from, temp, 0, length);
		if(Arrays.equals(key, temp)) {
			foundTimestamp = true;
		}
		return true;
	}

	@Override
	public boolean onValue(char[] value, int from, int length) {
		if(!foundTimestamp) {
			return true;
		}
		try {
			result = Long.parseLong(new String(value, from, length).trim());
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			result = System.currentTimeMillis();
		}
		return false;
	}

	@Override
	public boolean onReadEnd() {
		return true;
	}

	@Override
	public boolean onObjectBegin() {
		return true;
	}

	@Override
	public boolean onObjectEnd() {
		return true;
	}

	@Override
	public boolean onArrayBegin() {
		return true;
	}

	@Override
	public boolean onArrayEnd() {
		return true;
	}

	public long getResult() {
		return result;
	}
}
