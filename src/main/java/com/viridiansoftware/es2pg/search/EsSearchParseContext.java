/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

public abstract class EsSearchParseContext implements XContentGenerator, XContent {
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
	public boolean isPrettyPrint() {
		return false;
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
	public void writeFieldName(String name) throws IOException {
	}

	@Override
	public void writeNull() throws IOException {
	}

	@Override
	public void writeNullField(String name) throws IOException {
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
	}

	@Override
	public void writeNumber(double value) throws IOException {
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
	}

	@Override
	public void writeNumber(float value) throws IOException {
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
	}

	@Override
	public void writeNumber(int value) throws IOException {
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
	}

	@Override
	public void writeNumber(long value) throws IOException {
	}

	@Override
	public void writeNumber(short value) throws IOException {
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
	}

	@Override
	public void writeString(String value) throws IOException {
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
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

	@Override
	public void copyCurrentStructure(XContentParser parser) throws IOException {
	}

	@Override
	public boolean isClosed() {
		return closed;
	}
	
	@Override
	public XContentType type() {
		return XContentType.JSON;
	}

	@Override
	public byte streamSeparator() {
		return 0;
	}

	@Override
	public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes)
			throws IOException {
		return this;
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, String content) throws IOException {
		return XContentFactory.xContent(content).createParser(xContentRegistry, content);
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, InputStream is) throws IOException {
		return XContentFactory.xContent("{}").createParser(xContentRegistry, is);
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, byte[] data) throws IOException {
		return XContentFactory.xContent(data).createParser(xContentRegistry, data);
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, byte[] data, int offset, int length)
			throws IOException {
		return XContentFactory.xContent(data, offset, length).createParser(xContentRegistry, data, offset, length);
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, BytesReference bytes)
			throws IOException {
		return XContentFactory.xContent(bytes).createParser(xContentRegistry, bytes);
	}

	@Override
	public XContentParser createParser(NamedXContentRegistry xContentRegistry, Reader reader) throws IOException {
		return XContentFactory.xContent("{}").createParser(xContentRegistry, reader);
	}
}