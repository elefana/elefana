/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;

public class NoAllocJsonFlatten implements NoAllocJsonReader.JsonReaderListener {
	private final NoAllocJsonReader jsonReader = new NoAllocJsonReader();

	private PooledStringBuilder result;

	private char [] keyBuffer = new char[128];
	private int keyBufferLength = 0;

	private int [] arrayContextStack = new int[32];
	private int arrayContextStackIndex = 0;

	private int [] underscorePositionStack = new int[32];
	private int underscoreStackIndex = 0;

	private int totalValuesAppended = 0;

	private boolean valueWritten = false;

	public void flatten(PooledStringBuilder input, PooledStringBuilder output) {
		this.result = output;

		keyBufferLength = 0;
		totalValuesAppended = 0;
		underscoreStackIndex = 0;
		arrayContextStackIndex = 0;

		output.append('{');
		jsonReader.read(input, this);
		output.append('}');
		this.result = null;
	}

	private void pushUnderscore(boolean arrayContext) {
		if(underscoreStackIndex > 0) {
			if(isArrayContext()) {
				appendToKeyBuffer(getArrayIndex());
				appendToKeyBuffer('_');
				incrementArrayIndex();
			} else {
				appendToKeyBuffer('_');
			}
		}
		ensureUnderscoreStackCapacity(underscoreStackIndex);
		ensureArrayContextStackCapacity(arrayContextStackIndex);

		underscorePositionStack[underscoreStackIndex] = keyBufferLength;
		underscoreStackIndex++;

		arrayContextStack[arrayContextStackIndex] = arrayContext ? 0 : -1;
		arrayContextStackIndex++;
	}

	private void popUnderscore() {
		if(underscoreStackIndex > 0) {
			underscoreStackIndex--;
			keyBufferLength = underscorePositionStack[underscoreStackIndex];

			arrayContextStackIndex--;
		} else {
			keyBufferLength = 0;
		}
	}

	private void resetKey() {
		if(underscoreStackIndex > 0) {
			keyBufferLength = underscorePositionStack[underscoreStackIndex - 1];
		} else {
			keyBufferLength = 0;
		}
	}

	private boolean isArrayContext() {
		if(arrayContextStackIndex > 0) {
			return arrayContextStack[arrayContextStackIndex - 1] > -1;
		}
		return false;
	}

	private int getArrayIndex() {
		if(arrayContextStackIndex > 0) {
			if(arrayContextStack[arrayContextStackIndex - 1] > -1) {
				return arrayContextStack[arrayContextStackIndex - 1];
			}
		}
		return 0;
	}

	private void incrementArrayIndex() {
		if(arrayContextStackIndex > 0) {
			if(arrayContextStack[arrayContextStackIndex - 1] > -1) {
				arrayContextStack[arrayContextStackIndex - 1]++;
			}
		}
	}

	@Override
	public boolean onReadBegin() {
		return true;
	}

	@Override
	public boolean onReadEnd() {
		return true;
	}

	@Override
	public boolean onObjectBegin() {
		pushUnderscore(false);
		return true;
	}

	@Override
	public boolean onObjectEnd() {
		popUnderscore();
		resetKey();
		return true;
	}

	@Override
	public boolean onArrayBegin() {
		pushUnderscore(true);
		return true;
	}

	@Override
	public boolean onArrayEnd() {
		popUnderscore();

		if(!valueWritten) {
			if(totalValuesAppended > 0) {
				result.append(',');
			}
			result.append('"');
			result.append(keyBuffer, 0, keyBufferLength - 1);
			result.append('"');
			result.append(":null");
			valueWritten = true;
		}
		resetKey();
		return true;
	}

	@Override
	public boolean onKey(char[] value, int from, int length) {
		appendToKeyBuffer(value, from, length);
		valueWritten = false;
		return true;
	}

	@Override
	public boolean onValue(char[] value, int from, int length) {
		if(totalValuesAppended > 0) {
			result.append(',');
		}
		result.append('"');
		result.append(keyBuffer, 0, keyBufferLength);
		if(isArrayContext()) {
			result.append(getArrayIndex());
			incrementArrayIndex();
		}
		result.append('"');
		result.append(':');

		int startOffset = 0;
		int trimLength = length;
		for(int i = 0; i < length; i++) {
			if(value[from + i] == ' ') {
				continue;
			}
			startOffset = i;
			break;
		}
		for(int i = length; i > 0; i--) {
			if(value[from + i - 1] == ' ') {
				continue;
			}
			trimLength = i;
			break;
		}
		trimLength -= startOffset;

		result.append(value, from + startOffset, trimLength);

		totalValuesAppended++;
		resetKey();
		valueWritten = true;
		return true;
	}

	private void appendToKeyBuffer(char c) {
		ensureKeyBufferCapacity(keyBufferLength + 1);
		keyBuffer[keyBufferLength] = c;
		keyBufferLength++;
	}

	private void appendToKeyBuffer(int value) {
		if(value < 10 && value >= 0) {
			ensureKeyBufferCapacity(keyBufferLength + 1);
			keyBuffer[keyBufferLength] = (char)(value + '0');
			keyBufferLength++;
		} else {
			final String valueAsString = String.valueOf(value);
			ensureKeyBufferCapacity(keyBufferLength + valueAsString.length());
			for(int i = 0; i < valueAsString.length(); i++) {
				keyBuffer[keyBufferLength] = valueAsString.charAt(i);
				keyBufferLength++;
			}
		}
	}

	private void appendToKeyBuffer(char [] chars, int from, int length) {
		ensureKeyBufferCapacity(keyBufferLength + length);
		System.arraycopy(chars, from, keyBuffer, keyBufferLength, length);
		keyBufferLength += length;
	}

	private void ensureKeyBufferCapacity(int capacity) {
		if(keyBuffer.length > capacity) {
			return;
		}
		final char [] newBuffer = new char[capacity * 2];
		System.arraycopy(keyBuffer, 0, newBuffer, 0, keyBuffer.length);
		keyBuffer = newBuffer;
	}

	private void ensureUnderscoreStackCapacity(int capacity) {
		if(underscorePositionStack.length > capacity) {
			return;
		}
		final int [] newBuffer = new int[capacity * 2];
		System.arraycopy(underscorePositionStack, 0, newBuffer, 0, underscorePositionStack.length);
		underscorePositionStack = newBuffer;
	}

	private void ensureArrayContextStackCapacity(int capacity) {
		if(arrayContextStack.length > capacity) {
			return;
		}
		final int [] newBuffer = new int[capacity * 2];
		System.arraycopy(arrayContextStack, 0, newBuffer, 0, arrayContextStack.length);
		arrayContextStack = newBuffer;
	}
}
