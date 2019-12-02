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
package com.elefana.util;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties;

import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PooledStringBuilder implements Serializable, Appendable, CharSequence {
	private static final Queue<PooledStringBuilder> POOL = new ConcurrentLinkedQueue<PooledStringBuilder>();

	private final StringBuilder backingBuilder = new StringBuilder(32);

	public void release() {
		backingBuilder.setLength(0);
		POOL.offer(this);
	}

	public static PooledStringBuilder allocate() {
		final PooledStringBuilder result = POOL.poll();
		if(result == null) {
			return new PooledStringBuilder();
		}
		return result;
	}

	@Override
	public Appendable append(CharSequence csq) throws IOException {
		backingBuilder.append(csq);
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) throws IOException {
		backingBuilder.append(csq, start, end);
		return this;
	}

	@Override
	public Appendable append(char c) {
		backingBuilder.append(c);
		return this;
	}

	@Override
	public int length() {
		return backingBuilder.length();
	}

	@Override
	public char charAt(int index) {
		return backingBuilder.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return backingBuilder.subSequence(start, end);
	}

	public PooledStringBuilder append(Object obj) {
		backingBuilder.append(obj);
		return this;
	}

	public PooledStringBuilder append(String str) {
		backingBuilder.append(str);
		return this;
	}

	public PooledStringBuilder append(char[] str) {
		backingBuilder.append(str);
		return this;
	}

	public PooledStringBuilder append(boolean b) {
		backingBuilder.append(b);
		return this;
	}

	public PooledStringBuilder append(int i) {
		backingBuilder.append(i);
		return this;
	}

	public PooledStringBuilder append(StringBuilder builder) {
		backingBuilder.append(builder);
		return this;
	}

	public PooledStringBuilder append(PooledStringBuilder builder) {
		backingBuilder.append(builder.toString());
		return this;
	}

	@Override
	public String toString() {
		return backingBuilder.toString();
	}
}
