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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class NoAllocStringReplace {
	public static final CumulativeAverage AVG_ARRAY_SIZE = new CumulativeAverage(256);

	private static final Lock LOCK = new ReentrantLock();
	private static final List<NoAllocStringReplace> POOL = new ArrayList<NoAllocStringReplace>();

	public static NoAllocStringReplace allocate(String value) {
		LOCK.lock();
		NoAllocStringReplace result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		if(result == null) {
			result = new NoAllocStringReplace();
		}
		result.set(value);
		return result;
	}

	private char [] str;
	private int length;

	private NoAllocStringReplace() {
		super();
		str = new char[AVG_ARRAY_SIZE.avg()];
	}

	public void set(String value) {
		if(value.length() > str.length) {
			str = new char[value.length() + (value.length() / 2)];
		}
		length = value.length();
		value.getChars(0, value.length(), str, 0);
	}

	public void escapeUnicode(int index) {
		if(index > length - 5) {
			return;
		}

		boolean match = false;

		if(str[index] == '\\') {
			switch(str[index + 1]) {
			case 'u':
				if(index > 0 && str[index - 1] == '\\') {
					return;
				}
				match = true;
				for(int j = index + 2; j <= index + 5 && j < str.length; j++) {
					if(!Character.isDigit(str[j])) {
						match = false;
						break;
					}
				}
				break;
			}
		}

		if(match) {
			insert(index, 1, "\\");
		}
	}

	public void replaceAndEscapeUnicode(String [] search, String [] replace) {
		if(search.length != replace.length) {
			throw new RuntimeException("search and replace arrays must be same length");
		}
		boolean match = true;

		for(int i = 0; i < length; i++) {
			for(int j = 0; j < search.length; j++) {
				if(search[j] == null || replace[j] == null) {
					continue;
				}
				if(search[j].isEmpty()) {
					continue;
				}
				if(search[j].length() > length - i) {
					continue;
				}
				match = true;

				for(int k = 0; k < search[j].length() && i + k < length; k++) {
					if(str[i + k] != search[j].charAt(k)) {
						match = false;
						break;
					}
				}
				if(match) {
					replace(i, search[j], replace[j]);
					i += replace[j].length() - 1;
				}
			}
			escapeUnicode(i);
		}
	}

	public void insert(int index, int shift, String value) {
		final int oldLength = length;
		length += shift;
		//Shift chars to right
		char [] oldStr = str;
		if(str.length < length) {
			str = new char[length * 2];
			if(index > 0) {
				System.arraycopy(oldStr, 0, str, 0, index);
			}
		}
		final int remainder = oldLength - (index + (value.length() - shift));
		System.arraycopy(oldStr, index + (value.length() - shift), str,
				(index + value.length()), remainder);
		value.getChars(0, value.length(), str, index);
	}

	public void replace(int index, String search, String replace) {
		if(search.length() == replace.length()) {
			//Direct replace
			replace.getChars(0, replace.length(), str, index);
		} else if(search.length() < replace.length()) {
			insert(index, replace.length() - search.length(), replace);
		} else if(search.length() > replace.length()) {
			//Shift chars to left
			final int oldLength = length;
			length -= search.length() - replace.length();

			final int remainder = oldLength - (index + search.length());
			if(remainder < 0) {
				return;
			}
			replace.getChars(0, replace.length(), str, index);
			System.arraycopy(str, index + search.length(), str,
					index + replace.length(), remainder);
		}
	}

	public void dispose() {
		if(length > 0) {
			AVG_ARRAY_SIZE.add(str.length / length > 2 ? length : str.length);
		}

		LOCK.lock();
		POOL.add(this);
		LOCK.unlock();
	}

	public String disposeWithResult() {
		if(length <= 0) {
			dispose();
			return "";
		} else {
			final String result = new String(str, 0, length);
			dispose();
			return result;
		}
	}

	public char[] getCharArray() {
		return str;
	}

	public int getContentLength() {
		return length;
	}

	public static boolean contains(String str, String [] search) {
		if(str == null || search == null)
			return false;

		for(int i = 0; i < str.length(); i++) {
			for(int j = 0; j < search.length; j++) {
				if(search[j] == null) {
					continue;
				}
				if(search[j].isEmpty()) {
					continue;
				}

				boolean match = true;
				for(int k = 0; k < search[j].length() && i + k < str.length(); k++) {
					if(str.charAt(i + k) != search[j].charAt(k)) {
						match = false;
						break;
					}
				}
				if(match) {
					return true;
				}
			}
		}
		return false;

		/*if(str == null || search == null)
			return false;

		return Stream.of(search)
				.filter(Objects::nonNull)
				.filter(s -> !s.isEmpty())
				.anyMatch(str::contains);*/
	}
}
