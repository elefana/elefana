/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;

import java.util.ArrayList;
import java.util.List;

public class NoAllocJsonReader {
	private char [] value;
	private int length;

	private List<State> stateStack = new ArrayList<State>();
	private int readIndex;

	private boolean isArray = false;

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

		if(!listener.onReadBegin()) {
			return;
		}
		State state = State.INIT;
		while(readIndex < length) {
			System.out.println(state);
			switch(state) {
			case INIT:
				state = readObjectBegin(null, listener);
				break;
			case OBJECT_BEGIN:
				state = readObjectBegin(state, listener);
				break;
			case OBJECT_END:
				state = readObjectEnd(listener);
				break;
			case ARRAY_BEGIN:
				state = readArrayBegin(listener);
				break;
			case ARRAY_END:
				state = readArrayEnd(listener);
				break;
			case KEY:
				state = readKey(listener);
				break;
			case VALUE:
				state = readValue(listener);
				break;
			case TERMINATE:
				readIndex = length;
				break;
			}
		}
		listener.onReadEnd();
	}

	private State readObjectBegin(State previousState, JsonReaderListener listener) {
		while(readIndex < length) {
			switch(value[readIndex]) {
			case ',':
				return State.KEY;
			case ']':
				return State.ARRAY_END;
			case '{':
				if(!listener.onObjectBegin()) {
					return State.TERMINATE;
				}
				readIndex++;

				if(previousState != null) {
					stateStack.add(previousState);
				}
				return State.KEY;
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private State readObjectEnd(JsonReaderListener listener) {
		while(readIndex < length) {
			switch(value[readIndex]) {
			case '}':
				if(!listener.onObjectEnd()) {
					return State.TERMINATE;
				}
				readIndex++;
				if(stateStack.isEmpty()) {
					return State.TERMINATE;
				}
				return stateStack.remove(stateStack.size() - 1);
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private State readKey(JsonReaderListener listener) {
		int keyStart = -1;
		char startChar = ' ';
		while(readIndex < length) {
			switch(value[readIndex]) {
			case '{':
				if(keyStart == -1) {
					return State.OBJECT_BEGIN;
				}
				break;
			case '}':
				if(keyStart == -1) {
					return State.OBJECT_END;
				}
				break;
			case '[':
				if(keyStart == -1) {
					return State.ARRAY_BEGIN;
				}
				break;
			case ']':
				if(keyStart == -1) {
					return State.ARRAY_END;
				}
				break;
			case '"':
			case '\'':
				if(keyStart == -1) {
					keyStart = readIndex + 1;
					startChar = value[readIndex];
				} else if(value[readIndex] == startChar) {
					if(!listener.onKey(value, keyStart, readIndex - keyStart)) {
						return State.TERMINATE;
					}
					readIndex++;
					return State.VALUE;
				}
				break;
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private State readValue(JsonReaderListener listener) {
		int valueStart = readIndex;
		while(readIndex < length) {
			switch(value[readIndex]) {
			case '{':
				return State.OBJECT_BEGIN;
			case '[':
				return State.ARRAY_BEGIN;
			case ':':
				valueStart = readIndex + 1;
				break;
			case '}':
				if(readIndex == valueStart) {
					return State.OBJECT_END;
				}
				if(!listener.onValue(value, valueStart, readIndex - valueStart)) {
					return State.TERMINATE;
				}
				return isArray ? State.VALUE : State.KEY;
			case ']':
				if(readIndex == valueStart) {
					return State.ARRAY_END;
				}
				if(!listener.onValue(value, valueStart, readIndex - valueStart)) {
					return State.TERMINATE;
				}
				return isArray ? State.VALUE : State.KEY;
			case ',':
				if(!listener.onValue(value, valueStart, readIndex - valueStart)) {
					return State.TERMINATE;
				}
				readIndex++;
				return isArray ? State.VALUE : State.KEY;
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private State readArrayBegin(JsonReaderListener listener) {
		while(readIndex < length) {
			switch(value[readIndex]) {
			case '[':
				if(!listener.onArrayBegin()) {
					return State.TERMINATE;
				}
				readIndex++;
				isArray = true;
				return State.VALUE;
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private State readArrayEnd(JsonReaderListener listener) {
		while(readIndex < length) {
			switch(value[readIndex]) {
			case ']':
				if(!listener.onArrayEnd()) {
					return State.TERMINATE;
				}
				readIndex++;
				isArray = false;
				return State.KEY;
			}
			readIndex++;
		}
		return State.TERMINATE;
	}

	private enum State {
		INIT,
		OBJECT_BEGIN,
		OBJECT_END,
		ARRAY_BEGIN,
		ARRAY_END,
		KEY,
		VALUE,
		TERMINATE
	}

	public interface JsonReaderListener {

		public boolean onReadBegin();

		public boolean onReadEnd();

		public boolean onObjectBegin();

		public boolean onObjectEnd();

		public boolean onArrayBegin();

		public boolean onArrayEnd();

		public boolean onKey(char [] value, int from, int length);

		public boolean onValue(char [] value, int from, int length);
	}
}
