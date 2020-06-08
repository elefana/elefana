/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NoAllocMinMaxWord {
	public static final AtomicInteger MAX_ARRAY_SIZE = new AtomicInteger(256);

	private static final Lock LOCK = new ReentrantLock();
	private static final List<NoAllocMinMaxWord> POOL = new ArrayList<NoAllocMinMaxWord>();

	public static NoAllocMinMaxWord allocate(String value) {
		LOCK.lock();
		NoAllocMinMaxWord result = POOL.isEmpty() ? null : POOL.remove(0);
		LOCK.unlock();
		if(result == null) {
			result = new NoAllocMinMaxWord();
		}
		result.set(value);
		return result;
	}

	private final String cachedStr = new String();
	private char [] str;
	private int length;

	private NoAllocMinMaxWord() {
		str = new char[MAX_ARRAY_SIZE.get()];
	}

	public void dispose() {
		LOCK.lock();
		POOL.add(this);
		LOCK.unlock();
	}

	public void set(String value) {
		if(value == null) {
			length = 0;
			return;
		}

		MAX_ARRAY_SIZE.set(Math.max(value.length(), MAX_ARRAY_SIZE.get()));
		if(str == null) {
			str = new char[MAX_ARRAY_SIZE.get()];
		} else if(str.length < value.length()) {
			str = new char[MAX_ARRAY_SIZE.get()];
		}
		value.getChars(0, value.length(), str, 0);
		length = value.length();
	}

	public String getMin(String min) {
		if(this.length == 0) {
			return min;
		}

		int cursor = 0;
		int length = 0;

		int resultCursor = 0;
		int resultLength = 0;

		int firstWordCursor = -1;
		int firstWordLength = -1;

		if(min != null) {
			boolean allSpaces = true;

			for(int i = 0; i < this.length + 1; i++) {
				length++;
				if(i < this.length && !Character.isSpaceChar(str[i]) && str[i] != '\n' && str[i] != '\t') {
					allSpaces = false;
					if(firstWordCursor == -1) {
						firstWordCursor = i;
					}
					continue;
				}
				length--;

				if(length == 0) {
					cursor++;
					continue;
				}

				if(firstWordLength == -1) {
					firstWordLength = length;
				}

				if(!isLessThan(cursor, length, min)) {
					cursor = i + 1;
					length = 0;
					continue;
				}
				if(!isLessThan(cursor, length, resultCursor, resultLength)) {
					cursor = i + 1;
					length = 0;
					continue;
				}
				resultCursor = cursor;
				resultLength = length;
				cursor = i + 1;
				length = 0;
			}

			if(allSpaces) {
				return min;
			}
		}

		if(resultLength == 0) {
			if(firstWordCursor != -1) {
				resultCursor = firstWordCursor;
				resultLength = firstWordLength != -1 ? firstWordLength : this.length - firstWordCursor;
			} else {
				resultCursor = 0;
				resultLength = this.length;
			}
		}
		if(!isLessThan(resultCursor, resultLength, min)) {
			return min;
		}
		return new String(str, resultCursor, resultLength);
	}

	private boolean isLessThan(int cursor, int length, String min) {
		if(min == null) {
			return true;
		}
		int len1 = length;
		int len2 = min.length();
		int lim = Math.min(len1, len2);

		int k = 0;
		while (k < lim) {
			char c1 = str[cursor + k];
			char c2 = min.charAt(k);
			if (c1 != c2) {
				return c1 - c2 < 0;
			}
			k++;
		}
		return len1 - len2 < 0;
	}

	private boolean isLessThan(int cursor, int length, int minCursor, int minLength) {
		if(minLength == 0) {
			return true;
		}
		int len1 = length;
		int len2 = minLength;
		int lim = Math.min(len1, len2);

		int k = 0;
		while (k < lim) {
			char c1 = str[cursor + k];
			char c2 = str[minCursor + k];
			if (c1 != c2) {
				return c1 - c2 < 0;
			}
			k++;
		}
		return len1 - len2 < 0;
	}

	public String getMax(String max) {
		if(this.length == 0) {
			return max;
		}

		int cursor = 0;
		int length = 0;

		int resultCursor = 0;
		int resultLength = 0;

		int firstWordCursor = -1;
		int firstWordLength = -1;

		if(max != null) {
			boolean allSpaces = true;

			for(int i = 0; i < this.length + 1; i++) {
				length++;
				if(i < this.length && !Character.isSpaceChar(str[i]) && str[i] != '\n' && str[i] != '\t') {
					allSpaces = false;
					if(firstWordCursor == -1) {
						firstWordCursor = i;
					}
					continue;
				}
				length--;

				if(length == 0) {
					cursor++;
					continue;
				}

				if(firstWordLength == -1) {
					firstWordLength = length;
				}

				if(!isGreaterThan(cursor, length, max)) {
					cursor = i + 1;
					length = 0;
					continue;
				}
				if(!isGreaterThan(cursor, length, resultCursor, resultLength)) {
					cursor = i + 1;
					length = 0;
					continue;
				}
				resultCursor = cursor;
				resultLength = length;
				cursor = i + 1;
				length = 0;
			}

			if(allSpaces) {
				return max;
			}
		}

		if(resultLength == 0) {
			if(firstWordCursor != -1) {
				resultCursor = firstWordCursor;
				resultLength = firstWordLength != -1 ? firstWordLength : this.length - firstWordCursor;
			} else {
				resultCursor = 0;
				resultLength = this.length;
			}
		}
		if(!isGreaterThan(resultCursor, resultLength, max)) {
			return max;
		}
		return new String(str, resultCursor, resultLength);
	}

	private boolean isGreaterThan(int cursor, int length, String max) {
		if(max == null) {
			return true;
		}
		int len1 = length;
		int len2 = max.length();
		int lim = Math.min(len1, len2);

		int k = 0;
		while (k < lim) {
			char c1 = str[cursor + k];
			char c2 = max.charAt(k);
			if (c1 != c2) {
				return c1 - c2 > 0;
			}
			k++;
		}
		return len1 - len2 > 0;
	}

	private boolean isGreaterThan(int cursor, int length, int maxCursor, int maxLength) {
		if(maxLength == 0) {
			return true;
		}
		int len1 = length;
		int len2 = maxLength;
		int lim = Math.min(len1, len2);

		int k = 0;
		while (k < lim) {
			char c1 = str[cursor + k];
			char c2 = str[maxCursor + k];
			if (c1 != c2) {
				return c1 - c2 > 0;
			}
			k++;
		}
		return len1 - len2 > 0;
	}
}
