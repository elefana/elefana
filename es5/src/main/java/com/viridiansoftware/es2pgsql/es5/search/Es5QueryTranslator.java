/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es5.search;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.viridiansoftware.es2pgsql.es5.search.query.BoolQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.ConstantScoreQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.Es5QuerySpec;
import com.viridiansoftware.es2pgsql.es5.search.query.ExistsQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.MatchAllQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.MatchPhrasePrefixQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.MatchPhraseQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.MatchQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.MultiMatchQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.PrefixQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.RangeQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.RegexpQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.TermQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.TypeQuery;
import com.viridiansoftware.es2pgsql.es5.search.query.WildcardQuery;
import com.viridiansoftware.es2pgsql.exception.UnsupportedQueryTypeException;
import com.viridiansoftware.es2pgsql.search.QueryTranslator;
import com.viridiansoftware.es2pgsql.search.query.QuerySpec;
import com.viridiansoftware.es2pgsql.search.query.QueryType;

public class Es5QueryTranslator extends Es5QuerySpec implements QueryTranslator {
	protected Es5QuerySpec querySpec = null;
	
	@Override
	public boolean isMatchAllQuery() {
		if(querySpec == null) {
			return true;
		}
		return querySpec.isMatchAllQuery();
	}
	
	@Override
	public String toSqlWhereClause() {
		if(querySpec == null) {
			return "";
		}
		return querySpec.toSqlWhereClause();
	}

	@Override
	public void close() throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.close();
	}

	@Override
	public void flush() throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.flush();
	}

	@Override
	public XContentType contentType() {
		return XContentType.JSON;
	}

	@Override
	public void usePrettyPrint() {}

	@Override
	public boolean isPrettyPrint() {
		return false;
	}

	@Override
	public void usePrintLineFeedAtEnd() {}

	@Override
	public void writeStartObject() throws IOException {
		super.writeStartObject();
		if(querySpec == null) {
			return;
		}
		querySpec.writeStartObject();
	}

	@Override
	public void writeEndObject() throws IOException {
		super.writeEndObject();
		if(querySpec == null) {
			return;
		}
		querySpec.writeEndObject();
	}

	@Override
	public void writeStartArray() throws IOException {
		super.writeStartArray();
		if(querySpec == null) {
			return;
		}
		querySpec.writeStartArray();
	}

	@Override
	public void writeEndArray() throws IOException {
		super.writeEndArray();
		if(querySpec == null) {
			return;
		}
		querySpec.writeEndArray();
	}

	@Override
	public void writeFieldName(String name) throws IOException {
		if(querySpec == null) {
			//Start of query context
			querySpec = createQueryContext(name);
		} else {
			querySpec.writeFieldName(name);
		}
	}

	@Override
	public void writeNull() throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNull();
	}

	@Override
	public void writeNullField(String name) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNullField(name);
	}

	@Override
	public void writeBooleanField(String name, boolean value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeBooleanField(name, value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeBoolean(value);
	}

	@Override
	public void writeNumberField(String name, double value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(double value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, float value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(float value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, int value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(int value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumber(value);
	}

	@Override
	public void writeNumberField(String name, long value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumberField(name, value);
	}

	@Override
	public void writeNumber(long value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumber(value);
	}

	@Override
	public void writeNumber(short value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeNumber(value);
	}

	@Override
	public void writeStringField(String name, String value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeStringField(name, value);
	}

	@Override
	public void writeString(String value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeString(value);
	}

	@Override
	public void writeString(char[] text, int offset, int len) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeString(text, offset, len);
	}

	@Override
	public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeUTF8String(value, offset, length);
	}

	@Override
	public void writeBinaryField(String name, byte[] value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeBinaryField(name, value);
	}

	@Override
	public void writeBinary(byte[] value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeBinary(value);
	}

	@Override
	public void writeBinary(byte[] value, int offset, int length) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeBinary(value, offset, length);
	}

	@Override
	public void writeRawField(String name, InputStream value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawField(String name, BytesReference value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawField(name, value);
	}

	@Override
	public void writeRawField(String name, BytesReference value, XContentType xContentType) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawField(name, value, xContentType);
	}

	@Override
	public void writeRawValue(BytesReference value) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawValue(value);
	}

	@Override
	public void writeRawValue(BytesReference value, XContentType xContentType) throws IOException {
		if(querySpec == null) {
			return;
		}
		querySpec.writeRawValue(value, xContentType);
	}

	@Override
	public void copyCurrentStructure(XContentParser parser) throws IOException {
	}

	@Override
	public boolean isClosed() {
		if(querySpec == null) {
			return false;
		}
		return querySpec.isClosed();
	}

	private Es5QuerySpec createQueryContext(String name) {
		switch(QueryType.parse(name)) {
		case BOOL:
			return new BoolQuery();
		case BOOSTING:
			break;
		case COMMON:
			break;
		case CONSTANT_SCORE:
			return new ConstantScoreQuery();
		case DIS_MAX:
			break;
		case EXISTS:
			return new ExistsQuery();
		case FUNCTION_SCORE:
			break;
		case FUZZY:
			break;
		case HAS_CHILD:
			break;
		case HAS_PARENT:
			break;
		case IDS:
			break;
		case INDICES:
			break;
		case MATCH:
			return new MatchQuery();
		case MATCH_ALL:
			return new MatchAllQuery();
		case MATCH_PHRASE:
			return new MatchPhraseQuery();
		case MATCH_PHRASE_PREFIX:
			return new MatchPhrasePrefixQuery();
		case MORE_LIKE_THIS:
			break;
		case MULTI_MATCH:
			return new MultiMatchQuery();
		case NESTED:
			break;
		case PARENT_ID:
			break;
		case PERCOLATE:
			break;
		case PREFIX:
			return new PrefixQuery();
		case QUERY_STRING:
			break;
		case RANGE:
			return new RangeQuery();
		case REGEXP:
			return new RegexpQuery();
		case SCRIPT:
			break;
		case SIMPLE_QUERY_STRING:
			break;
		case TEMPLATE:
			break;
		case TERM:
			return new TermQuery();
		case TERMS:
			break;
		case TYPE:
			return new TypeQuery();
		case WILDCARD:
			return new WildcardQuery();
		case SPAN_CONTAINING:
		case SPAN_FIRST:
		case SPAN_MULTI:
		case SPAN_NEAR:
		case SPAN_NOT:
		case SPAN_OR:
		case SPAN_TERM:
		case SPAN_WITHIN:
		default:
			break;
		}
		throw new UnsupportedQueryTypeException();
	}
}
