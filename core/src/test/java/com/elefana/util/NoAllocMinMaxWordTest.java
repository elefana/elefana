/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class NoAllocMinMaxWordTest {
	private NoAllocMinMaxWord word = null;

	@After
	public void teardown() {
		if(word != null) {
			word.dispose();
		}
	}

	@Test
	public void testMinNull() {
		word = NoAllocMinMaxWord.allocate("Test");
		Assert.assertEquals("Test", word.getMin(null));
	}

	@Test
	public void testMinSingleWord() {
		word = NoAllocMinMaxWord.allocate("Def");
		Assert.assertEquals("Def", word.getMin("Xyz"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
	}

	@Test
	public void testMinSentence() {
		word = NoAllocMinMaxWord.allocate("Bcd Efg");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Efg Abc");
		Assert.assertEquals("Abc", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Bcd"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Xyz Abc Hij");
		Assert.assertEquals("Abc", word.getMin("Def"));
		Assert.assertEquals("Abc", word.getMin("Hij"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Efg Bcd ");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("  Efg Bcd ");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate(" Efg  Bcd  ");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("   ");
		Assert.assertEquals("Hij", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Hello there");
		Assert.assertEquals("Hello", word.getMin("aa"));
		Assert.assertEquals("Hello", word.getMin("hello"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("hello there");
		Assert.assertEquals("aa", word.getMin("aa"));
		Assert.assertEquals("hello", word.getMin("hello"));
	}

	@Test
	public void testMinSentenceWithNewLine() {
		word = NoAllocMinMaxWord.allocate("Bcd\nEfg");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Efg\nBcd\n");
		Assert.assertEquals("Bcd", word.getMin("Hij"));
		Assert.assertEquals("Abc", word.getMin("Abc"));
	}

	@Test
	public void testMaxNull() {
		word = NoAllocMinMaxWord.allocate("Test");
		Assert.assertEquals("Test", word.getMax(null));
	}

	@Test
	public void testMaxSingleWord() {
		word = NoAllocMinMaxWord.allocate("Def");
		Assert.assertEquals("Xyz", word.getMax("Xyz"));
		Assert.assertEquals("Def", word.getMax("Abc"));
	}

	@Test
	public void testMaxSentence() {
		word = NoAllocMinMaxWord.allocate("Efg Bcd ");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("Bcd Efg");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("  Efg Bcd ");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate(" Efg  Bcd  ");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("   ");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Abc", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("There Hello");
		Assert.assertEquals("zz", word.getMax("zz"));
		Assert.assertEquals("there", word.getMax("there"));
	}

	@Test
	public void testMaxSentenceWithNewLine() {
		word = NoAllocMinMaxWord.allocate("Bcd\nEfg");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
		word.dispose();

		word = NoAllocMinMaxWord.allocate("\nEfg\nBcd\n");
		Assert.assertEquals("Hij", word.getMax("Hij"));
		Assert.assertEquals("Efg", word.getMax("Abc"));
	}
}
