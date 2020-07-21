/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import com.elefana.api.util.PooledStringBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class NoAllocJsonReaderTest implements NoAllocJsonReader.JsonReaderListener {
	private final Map<String, Object> rootResults = new HashMap<String, Object>();
	private final NoAllocJsonReader reader = new NoAllocJsonReader();

	private String currentKey;

	private Stack<Map<String, Object>> currentObject = new Stack<>();
	private List<Object> currentArray;
	private boolean inArray = false;

	@Test
	public void testSimpleObject() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{\"key1\": 123, \"key2\": true, \"key3\": \"value\"}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals("123", rootResults.get("key1"));
		Assert.assertEquals("true", rootResults.get("key2"));
		Assert.assertEquals("value", rootResults.get("key3"));
	}

	@Test
	public void testMixedKeyStart() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{'key1': 123, \"key2\": true, 'key3': \"value\"}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals("123", rootResults.get("key1"));
		Assert.assertEquals("true", rootResults.get("key2"));
		Assert.assertEquals("value", rootResults.get("key3"));
	}

	@Test
	public void testObjectWithArray() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{'key1': 123, \"key2\": [1, \"mix\", 2]}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals("123", rootResults.get("key1"));
		final List<String> arrayResult = (List<String>) rootResults.get("key2");
		Assert.assertEquals("1", arrayResult.get(0));
		Assert.assertEquals("mix", arrayResult.get(1));
		Assert.assertEquals("2", arrayResult.get(2));
	}

	@Test
	public void testNestedObject() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{\"key1\": 123, \"key2\": {\"key3\":890, \"key4\": false}, \"key5\": \"end\"}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals("123", rootResults.get("key1"));
		final Map<String, Object> key2 = (Map<String, Object>) rootResults.get("key2");
		Assert.assertEquals("890", key2.get("key3"));
		Assert.assertEquals("false", key2.get("key4"));
		Assert.assertEquals("end", rootResults.get("key5"));
	}

	@Test
	public void testObjectWithArrayOfObjects() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{'key1': 123, \"key2\": [{\"key3\": \"value3\"},{\"key4\": \"value4\"}], \"key5\": \"end\"}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals("123", rootResults.get("key1"));
		final List<Object> arrayResult = (List<Object>) rootResults.get("key2");
		final Map<String, Object> obj1 = (Map<String, Object>) arrayResult.get(0);
		Assert.assertEquals("value3", obj1.get("key3"));

		final Map<String, Object> obj2 = (Map<String, Object>) arrayResult.get(1);
		Assert.assertEquals("value4", obj2.get("key4"));

		Assert.assertEquals("end", rootResults.get("key5"));
	}

	@Test
	public void testEscapedJson() {
		final PooledStringBuilder json = PooledStringBuilder.allocate("{\"key1\": \"{\\\"nestedKey\\\":\\\"nestedValue\\\"}\"}");
		reader.read(json, this);
		json.release();

		Assert.assertEquals(1, rootResults.size());
		Assert.assertEquals("{\\\"nestedKey\\\":\\\"nestedValue\\\"}", rootResults.get("key1"));
	}

	@Override
	public boolean onReadBegin() {
		return true;
	}

	@Override
	public boolean onReadEnd() {
		return true;
	}

	@Override
	public boolean onObjectBegin() {
		if(currentObject.isEmpty()) {
			currentObject.push(rootResults);
		} else {
			if(inArray) {
				final Map<String, Object> objectMap = new HashMap<String, Object>();
				currentArray.add(objectMap);
				currentObject.push(objectMap);
			} else {
				final Map<String, Object> objectMap = new HashMap<String, Object>();
				currentObject.peek().put(currentKey, objectMap);
				currentObject.push(objectMap);
			}
		}
		return true;
	}

	@Override
	public boolean onObjectEnd() {
		currentObject.pop();
		return true;
	}

	@Override
	public boolean onArrayBegin() {
		inArray = true;
		currentArray = new ArrayList<Object>();
		currentObject.peek().put(currentKey, currentArray);
		return true;
	}

	@Override
	public boolean onArrayEnd() {
		inArray = false;
		return true;
	}

	@Override
	public boolean onKey(char[] value, int from, int length) {
		currentKey = new String(value, from, length);
		currentObject.peek().put(currentKey, null);
		return true;
	}

	@Override
	public boolean onValue(char[] value, int from, int length) {
		String result = new String(value, from, length).trim();
		if(result.charAt(0) == '"') {
			result = result.substring(1, result.length() - 1);
		}
		if(inArray) {
			if(currentArray.size() > 0 && currentObject.peek().equals(currentArray.get(currentArray.size() - 1))) {
				currentObject.peek().put(currentKey, result);
			} else {
				currentArray.add(result);
			}
		} else {
			currentObject.peek().put(currentKey, result);
		}
		return true;
	}
}
