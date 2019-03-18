package com.elefana.util;

import java.util.concurrent.ConcurrentLinkedQueue;

public class NoAllocStringReplace {
	private static final CumulativeAverage AVG_ARRAY_SIZE = new CumulativeAverage(16);
	private static final ConcurrentLinkedQueue<NoAllocStringReplace> POOL = new ConcurrentLinkedQueue<NoAllocStringReplace>();

	public static NoAllocStringReplace allocate(String value) {
		NoAllocStringReplace result = POOL.poll();
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

	private void escapeUnicode(int index) {
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
		for(int i = 0; i < length; i++) {
			for(int j = 0; j < search.length; j++) {
				if(str[i] != search[j].charAt(0)) {
					continue;
				}

				boolean match = true;
				for(int k = 1; k < search[j].length() && i + k < length; k++) {
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

	private void insert(int index, int shift, String value) {
		final int oldLength = length;
		length += shift;
		//Shift chars to right
		char [] oldStr = str;
		if(str.length < length) {
			str = new char[length + (length / 2)];
			if(index > 0) {
				System.arraycopy(oldStr, 0, str, 0, index);
			}
		}
		final int remainder = oldLength - (index + (value.length() - shift));
		System.arraycopy(oldStr, index + (value.length() - shift), str,
				(index + value.length()), remainder);
		value.getChars(0, value.length(), str, index);
	}

	private void replace(int index, String search, String replace) {
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
			replace.getChars(0, replace.length(), str, index);
			System.arraycopy(str, index + search.length(), str,
					index + replace.length(), remainder);
		}
	}

	public String dispose() {
		AVG_ARRAY_SIZE.add(str.length / length > 2 ? length : str.length);
		final String result = new String(str, 0, length);
		POOL.offer(this);
		return result;
	}

	public static boolean contains(String str, String [] search) {
		for(int i = 0; i < str.length(); i++) {
			for(int j = 0; j < search.length; j++) {
				if(search[j].isEmpty()) {
					continue;
				}
				if(str.charAt(i) != search[j].charAt(0)) {
					continue;
				}
				boolean match = true;
				for(int k = 1; k < search[j].length() && i + k < str.length(); k++) {
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
	}
}
