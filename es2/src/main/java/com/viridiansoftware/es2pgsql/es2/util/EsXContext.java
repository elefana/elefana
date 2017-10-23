/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentString;
import org.elasticsearch.common.xcontent.XContentType;

public class EsXContext implements XContentGenerator, XContent {
	protected boolean closed = false;
	protected int startObjectCount = 0;
	protected int endObjectCount = 0;
	protected int startArrayCount = 0;
	protected int endArrayCount = 0;
	
	public boolean isContextFinished() {
		if(startObjectCount <= 0) {
			return false;
		}
		return startObjectCount == endObjectCount;
	}

	@Override
	public void close() throws IOException {
		closed = true;
	}

	@Override
	public void flush() throws IOException {
	}

	@Override
	public XContentType contentType() {
		return XContentType.JSON;
	}

	@Override
	public void usePrettyPrint() {
	}

	@Override
	public void usePrintLineFeedAtEnd() {
	}

	@Override
	public void writeStartObject() throws IOException {
		startObjectCount++;
	}

	@Override
	public void writeEndObject() throws IOException {
		endObjectCount++;
	}

	@Override
	public void writeStartArray() throws IOException {
		startArrayCount++;
	}

	@Override
	public void writeEndArray() throws IOException {
		endArrayCount++;
	}

	@Override
	public XContentGenerator createGenerator(OutputStream os) throws IOException {
		return this;
	}

	@Override
	public XContentGenerator createGenerator(OutputStream os, String[] filters) throws IOException {
		return this;
	}

	@Override
	public XContentParser createParser(String content) throws IOException {
		return XContentFactory.xContent(content).createParser(content);
	}

	@Override
	public XContentParser createParser(InputStream is) throws IOException {
		return XContentFactory.xContentType("{}").xContent().createParser(is);
	}

	@Override
	public XContentParser createParser(byte[] data) throws IOException {
		return XContentFactory.xContent(data).createParser(data);
	}

	@Override
	public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
		return XContentFactory.xContent(data, offset, length).createParser(data, offset, length);
	}

	@Override
	public XContentParser createParser(BytesReference bytes) throws IOException {
		return XContentFactory.xContent(bytes).createParser(bytes);
	}

	@Override
	public XContentParser createParser(Reader reader) throws IOException {
		return null;
	}

	@Override
	public void writeFieldName(XContentString name) throws IOException {
		writeFieldName(name.getValue());
	}

	@Override
	public void writeStringField(XContentString fieldName, String value) throws IOException {
		writeStringField(fieldName.getValue(), value);
	}

	@Override
	public void writeBooleanField(XContentString fieldName, boolean value) throws IOException {
		writeBooleanField(fieldName.getValue(), value);
	}

	@Override
	public void writeNullField(XContentString fieldName) throws IOException {
		writeNullField(fieldName.getValue());
	}

	@Override
	public void writeNumberField(XContentString fieldName, int value) throws IOException {
		writeNumberField(fieldName.getValue(), value);
	}

	@Override
	public void writeNumberField(XContentString fieldName, long value) throws IOException {
		writeNumberField(fieldName.getValue(), value);
	}

	@Override
	public void writeNumberField(XContentString fieldName, double value) throws IOException {
		writeNumberField(fieldName.getValue(), value);
	}

	@Override
	public void writeNumberField(XContentString fieldName, float value) throws IOException {
		writeNumberField(fieldName.getValue(), value);
	}

	@Override
	public void writeBinaryField(XContentString fieldName, byte[] data) throws IOException {
		writeBinaryField(fieldName.getValue(), data);
	}
	
	@Override
	public void writeArrayFieldStart(XContentString fieldName) throws IOException {
		writeArrayFieldStart(fieldName.getValue());
	}
	
	@Override
	public void writeObjectFieldStart(XContentString fieldName) throws IOException {
		writeObjectFieldStart(fieldName.getValue());
	}

	@Override
	public void writeArrayFieldStart(String fieldName) throws IOException {
		writeFieldName(fieldName);
		writeStartArray();
	}

	@Override
	public void writeObjectFieldStart(String fieldName) throws IOException {
		writeFieldName(fieldName);
		writeStartObject();
	}
	
	@Override
	public void writeFieldName(String name) throws IOException {
	}

	@Override
	public void writeString(String text) throws IOException {
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
	}

	@Override
	public void writeUTF8String(byte[] text, int offset, int length) throws IOException {
	}

	@Override
	public void writeBinary(byte[] data, int offset, int len) throws IOException {
	}

	@Override
	public void writeBinary(byte[] data) throws IOException {
	}

	@Override
	public void writeNumber(int v) throws IOException {
	}

	@Override
	public void writeNumber(long v) throws IOException {
	}

	@Override
	public void writeNumber(double d) throws IOException {
	}

	@Override
	public void writeNumber(float f) throws IOException {
	}

	@Override
	public void writeBoolean(boolean state) throws IOException {
	}

	@Override
	public void writeNull() throws IOException {
	}

	@Override
	public void writeStringField(String fieldName, String value) throws IOException {
	}

	@Override
	public void writeBooleanField(String fieldName, boolean value) throws IOException {
	}

	@Override
	public void writeNullField(String fieldName) throws IOException {
	}

	@Override
	public void writeNumberField(String fieldName, int value) throws IOException {
	}

	@Override
	public void writeNumberField(String fieldName, long value) throws IOException {
	}

	@Override
	public void writeNumberField(String fieldName, double value) throws IOException {
	}

	@Override
	public void writeNumberField(String fieldName, float value) throws IOException {
	}

	@Override
	public void writeBinaryField(String fieldName, byte[] data) throws IOException {
	}

	@Override
	public void writeRawField(String fieldName, InputStream content) throws IOException {
	}

	@Override
	public void writeRawField(String fieldName, BytesReference content) throws IOException {
	}

	@Override
	public void writeRawValue(BytesReference content) throws IOException {
	}

	@Override
	public void copyCurrentStructure(XContentParser parser) throws IOException {
	}
	
	@Override
	public XContentType type() {
		return XContentType.JSON;
	}

	@Override
	public byte streamSeparator() {
		return 0;
	}
}