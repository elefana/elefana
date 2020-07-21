/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.json.JsonUtils;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.indices.fieldstats.state.index.Index;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NoAllocJsonFlattenTest {

	@Test
	public void testSimpleJson() throws Exception {
		final String json = "{\"key1\": 123, \"key2\": true, \"key3\": \"value\"}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	@Test
	public void testNestedObjectJson() throws Exception {
		final String json = "{\"key1\": 123, \"key2\": {\"key3\":890, \"key4\": false}, \"key5\": \"end\"}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	@Test
	public void testArrayJson() throws Exception {
		final String json = "{'key1': 123, \"key2\": [1, \"mix\", 2]}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	@Test
	public void testNestedArrayJson() throws Exception {
		final String json = "{'key1': 123, \"key2\": [[1, 2, 3], [1, 2, 3], [1, 2, 3]]}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	@Test
	public void testNestedObjectArrayJson() throws Exception {
		final String json = "{'key1': 123, \"key2\": [{\"key3\": \"value3\"},{\"key4\": \"value4\"}], \"key5\": \"end\"}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	@Test
	public void testNestedObjectNestedArrayJson() throws Exception {
		final String json = "{'key1': 123, \"key2\": [{\"key3\": [1, 2, 3]},{\"key4\": \"value4\"}], \"key5\": \"end\"}";
		Assert.assertEquals(getLegacyFlattenedJson(json), getFlattenedJson(json));
	}

	private String getFlattenedJson(String json) throws IOException {
		final PooledStringBuilder input = PooledStringBuilder.allocate(json);
		final PooledStringBuilder output = PooledStringBuilder.allocate();
		IndexUtils.flattenJson(input, output);
		input.release();
		return output.toStringAndRelease();
	}

	private String getLegacyFlattenedJson(String json) {
		final NoAllocStringReplace str = NoAllocStringReplace.allocate(json);
		str.replaceAndEscapeUnicode(EscapeUtils.PSQL_ESCAPE_SEARCH, EscapeUtils.PSQL_ESCAPE_REPLACE);

		final StringBuilder result = POOLED_STRING_BUILDER.get();

		try {
			final JsonParser jsonParser = JsonUtils.JSON_FACTORY.createParser(str.getCharArray(), 0, str.getContentLength());
			jsonParser.nextToken();

			result.append('{');
			IndexUtils.flattenJsonObject(jsonParser, result, "");
			result.append('}');

			str.dispose();
			jsonParser.close();
		} catch (Exception e) {

		}

		return result.toString();
	}

	public static ThreadLocal<StringBuilder> POOLED_STRING_BUILDER = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder(1024);
		}

		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0); // clear/reset the buffer
			return b;
		}
	};
}
