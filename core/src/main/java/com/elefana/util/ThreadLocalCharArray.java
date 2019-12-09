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

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadLocalCharArray extends ThreadLocal<char[]> {
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
