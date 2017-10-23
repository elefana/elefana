/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search.agg;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.BucketAggregation;

public class Es5BucketAggregation extends Es5Aggregations implements BucketAggregation {
	protected final List<Aggregation> subaggregations = new ArrayList<Aggregation>(1);

	public Es5BucketAggregation(String aggregationName) {
		super(aggregationName);
	}
	
	protected Es5Aggregations getSubAggregation(int index) {
		return (Es5Aggregations) getSubAggregation(subaggregations.size() - 1);
	}
	
	@Override
	public void writeFieldName(String name) throws IOException {
		super.writeFieldName(name);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty() || getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			if(name.equals(KEY_AGGS)) {
				return;
			}
			subaggregations.add(new Es5BucketAggregation(name));
		} else {
			getSubAggregation(subaggregations.size() - 1).writeFieldName(name);
		}
	}
	
	@Override
	public void writeStartObject() throws IOException {
		super.writeStartObject();
		
		if(!isAggregationParsed()) {
			return;
		}
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeStartObject();
	}
	
	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();
		
		if(!isAggregationParsed()) {
			return;
		}
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeEndObject();
	}
	
	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
		
		if(!isAggregationParsed()) {
			return;
		}
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeStartArray();
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		
		if(!isAggregationParsed()) {
			return;
		}
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeEndArray();
	}
	
	@Override
	public void writeNull() throws IOException {
		super.writeNull();
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNull();
	}

	@Override
	public void writeNullField(String name) throws IOException {
		super.writeNullField(name);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNullField(name);
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		super.writeBooleanField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeBooleanField(name, value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		super.writeBoolean(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeBoolean(value);
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		super.writeNumberField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumberField(name, value);
	}

	@Override
	public void writeNumber(double value) throws IOException {
		super.writeNumber(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		super.writeNumberField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumberField(name, value);
	}

	@Override
	public void writeNumber(float value) throws IOException {
		super.writeNumber(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		super.writeNumberField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumberField(name, value);
	}

	@Override
	public void writeNumber(int value) throws IOException {
		super.writeNumber(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		super.writeNumberField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumberField(name, value);
	}

	@Override
	public void writeNumber(long value) throws IOException {
		super.writeNumber(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumber(value);
	}

	@Override
	public void writeNumber(short value) throws IOException {
		super.writeNumber(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeNumber(value);
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		super.writeStringField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeStringField(name, value);
	}

	@Override
	public void writeString(String value) throws IOException {
		super.writeString(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeString(value);
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
		super.writeString(text, offset, len);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeString(text, offset, len);
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
		super.writeUTF8String(value, offset, length);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeUTF8String(value, offset, length);
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
		super.writeBinaryField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeBinaryField(name, value);
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
		super.writeBinary(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeBinary(value);
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
		super.writeBinary(value, offset, length);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeBinary(value, offset, length);
	}

	@Override
	public void writeRawField(String name, InputStream value) throws IOException {
		super.writeRawField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
		super.writeRawField(name, value, xContentType);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawField(String name, BytesReference value) throws IOException {
		super.writeRawField(name, value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException {
		super.writeRawField(name, value, xContentType);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawValue(BytesReference value) throws IOException {
		super.writeRawValue(value);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawValue(value);
	}

	@Override
	public void writeRawValue(BytesReference value, XContentType xContentType) throws IOException {
		super.writeRawValue(value, xContentType);
		if(!isAggregationParsed()) {
			return;
		}
		
		if(subaggregations.isEmpty()) {
			return;
		}
		if(getSubAggregation(subaggregations.size() - 1).isContextFinished()) {
			return;
		}
		getSubAggregation(subaggregations.size() - 1).writeRawValue(value, xContentType);
	}
	
	@Override
	public List<Aggregation> getSubAggregations() {
		return subaggregations;
	}
}
