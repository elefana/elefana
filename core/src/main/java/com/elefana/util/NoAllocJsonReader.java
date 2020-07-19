/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;

public class NoAllocJsonReader {
	private char [] value;
	private int length;

	private int readIndex;

	private void init(PooledStringBuilder str) {
		length = str.length();

		if(value == null || value.length < length) {
			value = new char[length];
		}
		str.getChars(value);
		readIndex = 0;
	}

	public void read(PooledStringBuilder str, JsonReaderListener listener) {
		init(str);
	}

	public void readObject(JsonReaderListener listener) {
		State state = State.OBJECT_BEGIN;
		int keyBegin = 0;

		while(state != State.OBJECT_END && readIndex < length) {
			switch(value[readIndex]) {
			case '{':
				listener.onObjectBegin();
				readIndex++;
				readObject(listener);
				state = State.OBJECT_BEGIN;
				continue;
			case '}':
				listener.onObjectEnd();
				state = State.OBJECT_END;
				readIndex++;
				continue;
			case '"':
			case '\'':
				switch (state) {
				case OBJECT_BEGIN:
					state = State.KEY;
					keyBegin = readIndex + 1;
					break;
				case ARRAY_BEGIN:
					state = State.VALUE;
					break;
				case KEY:
					listener.onKey(value, keyBegin, readIndex - 1);
					state = State.VALUE;
					break;
				case VALUE:

					break;
				}
				readIndex++;
				continue;
			}
		}
	}

	private enum State {
		OBJECT_BEGIN,
		OBJECT_END,
		ARRAY_BEGIN,
		ARRAY_END,
		KEY,
		VALUE
	}

	public interface JsonReaderListener {

		public void onObjectBegin();

		public void onObjectEnd();

		public void onKey(char [] value, int from, int length);

		public void onValue(char [] value, int from, int length);
	}
}
