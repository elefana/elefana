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

import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.fieldstats.job.DocumentSourceProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NoAllocStringReplace {
	public static final AtomicInteger MAX_ARRAY_SIZE = new AtomicInteger(256);

	private static final char ESCAPE_CHAR = '\\';
	private static final String ESCAPE_STR = "\\";
	private static final char UNICODE_PREFIX = 'u';

	private static final Lock LOCK = new ReentrantLock();
	private static final List<NoAllocStringReplace> POOL = new ArrayList<NoAllocStringReplace>();

	public static NoAllocStringReplace allocate(PooledStringBuilder value) {
		LOCK.lock();
		NoAllocStringReplace result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		if(result == null) {
			result = new NoAllocStringReplace();
		}
		result.set(value);
		return result;
	}

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

	public static NoAllocStringReplace allocate(char [] value, int length) {
		LOCK.lock();
		NoAllocStringReplace result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		if(result == null) {
			result = new NoAllocStringReplace();
		}
		result.set(value, length);
		return result;
	}

	private char [] pooledStr;

	private char [] str;
	private int length;

	private NoAllocStringReplace() {
		super();
		str = new char[MAX_ARRAY_SIZE.get()];
	}

	public void set(PooledStringBuilder value) {
		if(value.length() > str.length) {
			str = new char[value.length() + 1];
		}
		length = value.length();
		value.getChars(0, value.length(), str, 0);
	}

	public void set(String value) {
		if(value.length() > str.length) {
			str = new char[value.length() + 1];
		}
		length = value.length();
		value.getChars(0, value.length(), str, 0);
	}

	public void set(char [] value, int length) {
		pooledStr = str;
		str = value;
		this.length = length;
	}

	public void escapeUnicode(int index) {
		if(index > length - 5) {
			return;
		}
		if(str[index] != ESCAPE_CHAR) {
			return;
		}
		if (str[index + 1] != UNICODE_PREFIX) {
			return;
		}
		if (index > 0 && str[index - 1] == ESCAPE_CHAR) {
			return;
		}

		for (int j = index + 2; j <= index + 5 && j < str.length; j++) {
			switch(str[j]) {
			case '\u0030':
			case '\u0031':
			case '\u0032':
			case '\u0033':
			case '\u0034':
			case '\u0035':
			case '\u0036':
			case '\u0037':
			case '\u0038':
			case '\u0039':
			case '\uFF10':
			case '\uFF11':
			case '\uFF12':
			case '\uFF13':
			case '\uFF14':
			case '\uFF15':
			case '\uFF16':
			case '\uFF17':
			case '\uFF18':
			case '\uFF19':
				continue;
			default:
				return;
			}
		}
		insert(index, 1, ESCAPE_STR);
	}

	public void replaceAndEscapeUnicode(String [] search, String [] replace) {
		if(search.length != replace.length) {
			throw new RuntimeException("search and replace arrays must be same length");
		}
		boolean match = true;

		final int searchArrayLength = search.length;
		int searchLength = 0;
		String searchStr = null;
		String replaceStr = null;

		for(int i = 0; i < length; i++) {
			for(int j = 0; j < searchArrayLength; j++) {
				searchStr = search[j];
				replaceStr = replace[j];
				if(searchStr == null || replaceStr == null) {
					continue;
				}
				if(searchStr.isEmpty()) {
					continue;
				}
				if(searchStr.length() > length - i) {
					continue;
				}
				match = true;
				searchLength = searchStr.length();

				for(int k = 0; k < searchLength && i + k < length; k++) {
					if(str[i + k] != searchStr.charAt(k)) {
						match = false;
						break;
					}
				}
				if(match) {
					replace(i, searchStr, replaceStr);
					i += replaceStr.length() - 1;
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
			MAX_ARRAY_SIZE.set(Math.max(length * 2, MAX_ARRAY_SIZE.get()));
			str = new char[MAX_ARRAY_SIZE.get()];
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
			MAX_ARRAY_SIZE.set(Math.max(length, MAX_ARRAY_SIZE.get()));
		}

		if(pooledStr != null) {
			str = pooledStr;
			pooledStr = null;
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
			for (String s : search) {
				if (s == null) {
					continue;
				}
				if (s.isEmpty()) {
					continue;
				}

				boolean match = true;
				for (int k = 0; k < s.length() && i + k < str.length(); k++) {
					if (str.charAt(i + k) != s.charAt(k)) {
						match = false;
						break;
					}
				}
				if (match) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean contains(DocumentSourceProvider documentSourceProvider, String [] search) {
		return contains(documentSourceProvider.getDocument(), documentSourceProvider.getDocumentLength(), search);
	}

	public static boolean contains(char [] str, int length, String [] search) {
		if(str == null || search == null)
			return false;

		for(int i = 0; i < length; i++) {
			for (String s : search) {
				if (s == null) {
					continue;
				}
				if (s.isEmpty()) {
					continue;
				}

				boolean match = true;
				for (int k = 0; k < s.length() && i + k < length; k++) {
					if (str[i + k] != s.charAt(k)) {
						match = false;
						break;
					}
				}
				if (match) {
					return true;
				}
			}
		}
		return false;
	}
}
