/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.search.agg;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import com.viridiansoftware.es2pg.exception.UnsupportedAggregationTypeException;
import com.viridiansoftware.es2pg.search.AggregationType;
import com.viridiansoftware.es2pg.search.EsSearchParseContext;

public class Aggregation extends EsSearchParseContext {
	private static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections.unmodifiableList(new ArrayList<Aggregation>(1));
	
	protected static final String KEY_AGGS = "aggregations";
	
	protected final String aggregationName;
	
	protected AggregationSpec aggregationContext;

	public Aggregation(String aggregationName) {
		super();
		this.aggregationName = aggregationName;
	}
	
	public String toSqlQuery(List<String> tempTablesCreated, String queryTable) {
		if(aggregationContext == null) {
			return "";
		}
		return aggregationContext.toSqlQuery(tempTablesCreated, queryTable, this);
	}
	
	@Override
	public void writeFieldName(String name) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			aggregationContext = createAggregationContext(name);
		} else {
			aggregationContext.writeFieldName(name);
		}
	}
	
	protected AggregationSpec createAggregationContext(String name) {
		switch(AggregationType.parse(name.toUpperCase())) {
		case ADJACENCY_MATRIX:
			break;
		case AVG:
			return new AvgAggregationSpec();
		case CARDINALITY:
			break;
		case CHILDREN:
			break;
		case DATE_HISTOGRAM:
			break;
		case DATE_RANGE:
			break;
		case DIVERSIFIED_SAMPLER:
			break;
		case EXTENDED_STATS:
			break;
		case FILTER:
			break;
		case FILTERS:
			break;
		case GEOHASH_GRID:
			break;
		case GEO_BOUNDS:
			break;
		case GEO_CENTROID:
			break;
		case GEO_DISTANCE:
			break;
		case GLOBAL:
			break;
		case HISTOGRAM:
			break;
		case IP_RANGE:
			break;
		case MAX:
			break;
		case MIN:
			break;
		case MISSING:
			break;
		case NESTED:
			break;
		case PERCENTILES:
			break;
		case PERCENTILE_RANKS:
			break;
		case RANGE:
			return new RangeAggregationSpec();
		case REVERSE_NESTED:
			break;
		case SAMPLER:
			break;
		case SCRIPTED_METRIC:
			break;
		case SIGNIFICANT_TERMS:
			break;
		case STATS:
			break;
		case SUM:
			break;
		case TERMS:
			break;
		case TOP_HITS:
			break;
		case VALUE_COUNT:
			break;
		default:
			break;
		}
		throw new UnsupportedAggregationTypeException();
	}
	
	@Override
	public void writeStartObject() throws IOException {
		super.writeStartObject();
		
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeStartObject();
	}
	
	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();
		
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeEndObject();
		return;
	}
	
	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
		
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeStartArray();
	}
	
	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeEndArray();
	}
	
	@Override
	public void writeNull() throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNull();
	}

	@Override
	public void writeNullField(String name) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNullField(name);
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeBooleanField(name, value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeBoolean(value);
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(double value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(float value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(int value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(long value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumber(value);
	}

	@Override
	public void writeNumber(short value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeNumber(value);
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeStringField(name, value);
	}

	@Override
	public void writeString(String value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeString(value);
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeString(text, offset, len);
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeUTF8String(value, offset, length);
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeBinaryField(name, value);
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeBinary(value);
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeBinary(value, offset, length);
	}

	@Override
	public void writeRawField(String name, InputStream value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawField(String name, BytesReference value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawValue(BytesReference value) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawValue(value);
	}

	@Override
	public void writeRawValue(BytesReference value, XContentType xContentType) throws IOException {
		if(isAggregationParsed()) {
			return;
		}
		if(aggregationContext == null) {
			return;
		}
		aggregationContext.writeRawValue(value, xContentType);
	}

	protected boolean isAggregationParsed() {
		if(aggregationContext == null) {
			return false;
		}
		return aggregationContext.isContextFinished();
	}
	
	public String getAggregationName() {
		return aggregationName;
	}
	
	public List<Aggregation> getSubAggregations() {
		return EMPTY_AGGREGATION_LIST;
	}
}
