/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;
import org.junit.Assert;
import org.junit.Test;

public class NoAllocTimestampExtractorTest {

	@Test
	public void testExtract() {
		final NoAllocTimestampExtractor extractor = new NoAllocTimestampExtractor("@timestamp");
		final PooledStringBuilder json = PooledStringBuilder.allocate("{\"test\":\"123\",\"test2\":[0,3, 4, 5], \"test3\":{\"key\":\"value\"}, \"@timestamp\": 123456789}");
		Assert.assertEquals(123456789L, extractor.extract(json));
		json.release();
	}

	@Test
	public void testExtractNotPresent() {
		final NoAllocTimestampExtractor extractor = new NoAllocTimestampExtractor("@timestamp");
		final PooledStringBuilder json = PooledStringBuilder.allocate("{\"test\":\"123\",\"test2\":[0,3, 4, 5], \"@tstmp\": 123456789}");
		Assert.assertEquals(System.currentTimeMillis(), extractor.extract(json), 1L);
		json.release();
	}
}
