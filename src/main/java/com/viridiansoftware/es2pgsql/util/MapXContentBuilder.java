/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

public class MapXContentBuilder extends EsXContext {
	private final Map<String, Object> result;

	private String currentFieldName;
	private List<Object> currentValueArray;
	private MapXContentBuilder currentObject;
	
	public MapXContentBuilder() {
		this(new HashMap<String, Object>());
	}
	
	public MapXContentBuilder(Map<String, Object> backingMap) {
		super();
		this.result = backingMap;
	}

	@Override
	public void writeStartObject() throws IOException {
		super.writeStartObject();
		if (currentObject != null) {
			currentObject.writeStartObject();
		} else if(currentFieldName != null) {
			currentObject = new MapXContentBuilder();
			currentObject.writeStartObject();
		}
	}

	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();
		if (currentObject == null) {
			return;
		}
		currentObject.writeEndObject();
		if(!currentObject.isContextFinished()) {
			return;
		}
		if(currentValueArray != null) {
			currentValueArray.add(currentObject.getResult());
		} else if(currentFieldName != null) {
			result.put(currentFieldName, currentObject.getResult());
		}
		currentObject = null;
	}

	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
		if (currentObject != null) {
			currentObject.writeStartArray();
		} else if (currentValueArray == null) {
			currentValueArray = new ArrayList<Object>();
		}
	}

	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		if (currentObject != null) {
			currentObject.writeEndArray();
		} else if (currentValueArray != null) {
			result.put(currentFieldName, currentValueArray);
			currentValueArray = null;
		}
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		if (currentObject != null) {
			currentObject.writeFieldName(name);
		} else {
			currentFieldName = name;
		}
	}

	@Override
	public void writeNull() throws IOException {
		if (currentObject != null) {
			currentObject.writeNull();
		} else if (currentValueArray != null) {
			currentValueArray.add(null);
		} else {
			result.put(currentFieldName, null);
		}
	}

	@Override
	public void writeNullField(String name) throws IOException {
		if (currentObject != null) {
			currentObject.writeNullField(name);
		} else {
			result.put(name, null);
		}
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		if (currentObject != null) {
			currentObject.writeBooleanField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		if (currentObject != null) {
			currentObject.writeBoolean(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumberField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeNumber(double value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumber(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumberField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeNumber(float value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumber(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumberField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeNumber(int value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumber(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumberField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeNumber(long value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumber(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeNumber(short value) throws IOException {
		if (currentObject != null) {
			currentObject.writeNumber(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		if (currentObject != null) {
			currentObject.writeStringField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeString(String value) throws IOException {
		if (currentObject != null) {
			currentObject.writeString(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
		if (currentObject != null) {
			currentObject.writeBinaryField(name, value);
		} else {
			result.put(name, value);
		}
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
		if (currentObject != null) {
			currentObject.writeBinary(value);
		} else if (currentValueArray != null) {
			currentValueArray.add(value);
		} else {
			result.put(currentFieldName, value);
		}
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
		if (currentObject != null) {
			currentObject.writeBinary(value, offset, length);
		} else if (currentValueArray != null) {
			for(int i = offset; i < value.length && i < offset + length; i++) {
				currentValueArray.add(value[i]);
			}
		} else {
			byte [] data = new byte[length];
			for(int i = offset; i < value.length && i < offset + length; i++) {
				data[i - offset] = value[i];
			}
			result.put(currentFieldName, data);
		}
	}

	@Override
	public void writeRawField(String name, InputStream value) throws IOException {
	}

	@Override
	public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
	}

	@Override
	public void writeRawField(String name, BytesReference value) throws IOException {
	}

	@Override
	public void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException {
	}

	@Override
	public void writeRawValue(BytesReference value) throws IOException {
	}

	@Override
	public void writeRawValue(BytesReference value, XContentType xContentType) throws IOException {
	}

	public Map<String, Object> getResult() {
		return result;
	}
}
