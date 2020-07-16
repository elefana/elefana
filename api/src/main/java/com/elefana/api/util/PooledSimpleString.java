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
package com.elefana.api.util;

public class PooledSimpleString {
	private char [] array;
	private int length;

	public PooledSimpleString(char[] array, int length) {
		this.array = array;
		this.length = length;
	}

	public static PooledSimpleString allocate() {
		return new PooledSimpleString(PooledCharArray.allocate(), 0);
	}

	public static PooledSimpleString copyOf(char [] original, int length) {
		char[] copy = PooledCharArray.allocate();
		if(copy.length < original.length) {
			copy = new char[original.length];
		}
		System.arraycopy(original, 0, copy, 0, length);
		return new PooledSimpleString(copy, length);
	}

	public static PooledSimpleString copyOf(String original) {
		char[] copy = PooledCharArray.allocate();
		if(copy.length < original.length()) {
			copy = new char[original.length()];
		}
		original.getChars(0, original.length(), copy, 0);
		return new PooledSimpleString(copy, original.length());
	}

	public void release() {
		PooledCharArray.release(array);
		array = null;
	}

	public char[] getArray() {
		return array;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}
}
