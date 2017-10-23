/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search.agg;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.jdbc.core.JdbcTemplate;

import com.viridiansoftware.es2pgsql.document.IndexFieldMappingService;
import com.viridiansoftware.es2pgsql.es5.util.EsXContext;
import com.viridiansoftware.es2pgsql.exception.UnsupportedAggregationTypeException;
import com.viridiansoftware.es2pgsql.search.AggregationType;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.Aggregation;
import com.viridiansoftware.es2pgsql.search.agg.AggregationExec;

public class Es5Aggregations extends EsXContext implements Aggregation {
	private static final List<Aggregation> EMPTY_AGGREGATION_LIST = Collections
			.unmodifiableList(new ArrayList<Aggregation>(1));

	protected static final String KEY_AGGS = "aggregations";

	protected final String aggregationName;

	protected Es5AggregationSpec aggregationSpec;

	public Es5Aggregations(String aggregationName) {
		super();
		this.aggregationName = aggregationName;
	}

	public void executeSqlQuery(final AggregationExec parentExec, final Map<String, Object> aggregationsResult,
			final String queryTable) {
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.executeSqlQuery(new AggregationExec(parentExec.getIndices(), parentExec.getTypes(),
				parentExec.getJdbcTemplate(), parentExec.getIndexFieldMappingService(), aggregationsResult,
				parentExec.getTempTablesCreated(), queryTable, parentExec.getRequestBodySearch(), this));
	}

	public void executeSqlQuery(final List<String> indices, final String[] types, final JdbcTemplate jdbcTemplate,
			final IndexFieldMappingService indexFieldMappingService, final Map<String, Object> aggregationsResult,
			final List<String> tempTablesCreated, final String queryTable, final RequestBodySearch requestBodySearch) {
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.executeSqlQuery(new AggregationExec(indices, types, jdbcTemplate, indexFieldMappingService,
				aggregationsResult, tempTablesCreated, queryTable, requestBodySearch, this));
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			aggregationSpec = createAggregationContext(name);
		} else {
			aggregationSpec.writeFieldName(name);
		}
	}

	protected Es5AggregationSpec createAggregationContext(String name) {
		switch (AggregationType.parse(name.toUpperCase())) {
		case ADJACENCY_MATRIX:
			break;
		case AVG:
			return new AvgAggregationSpec();
		case CARDINALITY:
			break;
		case CHILDREN:
			break;
		case DATE_HISTOGRAM:
			return new DateHistogramAggregationSpec();
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
			return new MaxAggregationSpec();
		case MIN:
			return new MinAggregationSpec();
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
			return new SumAggregationSpec();
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

		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeStartObject();
	}

	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();

		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeEndObject();
		return;
	}

	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();

		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeStartArray();
	}

	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();

		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeEndArray();
	}

	@Override
	public void writeNull() throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNull();
	}

	@Override
	public void writeNullField(String name) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNullField(name);
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeBooleanField(name, value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeBoolean(value);
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(double value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(float value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(int value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(long value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumber(value);
	}

	@Override
	public void writeNumber(short value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeNumber(value);
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeStringField(name, value);
	}

	@Override
	public void writeString(String value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeString(value);
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeString(text, offset, len);
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeUTF8String(value, offset, length);
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeBinaryField(name, value);
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeBinary(value);
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeBinary(value, offset, length);
	}

	@Override
	public void writeRawField(String name, InputStream value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawField(String name, BytesReference value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawValue(BytesReference value) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawValue(value);
	}

	@Override
	public void writeRawValue(BytesReference value, XContentType xContentType) throws IOException {
		if (isAggregationParsed()) {
			return;
		}
		if (aggregationSpec == null) {
			return;
		}
		aggregationSpec.writeRawValue(value, xContentType);
	}

	protected boolean isAggregationParsed() {
		if (aggregationSpec == null) {
			return false;
		}
		return aggregationSpec.isContextFinished();
	}

	public String getAggregationName() {
		return aggregationName;
	}

	public List<Aggregation> getSubAggregations() {
		return EMPTY_AGGREGATION_LIST;
	}
}
