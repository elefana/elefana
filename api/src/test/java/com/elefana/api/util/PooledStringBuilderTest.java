/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.api.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PooledStringBuilderTest {
	private static String DATA;
	private static ByteBuf BYTE_BUF;

	@BeforeClass
	public static void beforeClass() {
		final StringBuilder stringBuilder = new StringBuilder(4096);
		for(int i = 0; i < 1024; i++) {
			stringBuilder.append("Test " + i);
			if(i < 1024) {
				stringBuilder.append(',');
			}
		}
		DATA = stringBuilder.toString();
		BYTE_BUF = Unpooled.buffer(DATA.length() * 2);
		BYTE_BUF.writeBytes(StandardCharsets.UTF_8.encode(DATA));
	}

	@Test
	public void testAppendByteBuf() {
		final PooledStringBuilder result = PooledStringBuilder.allocate();
		result.append(BYTE_BUF, StandardCharsets.UTF_8);
		Assert.assertEquals(DATA, result.toString());
	}
}
