/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.api.node;

import org.junit.Assert;
import org.junit.Test;

import com.elefana.api.node.v2.V2NodeStats;
import com.jsoniter.JsonIterator;

public class NodesStatsResponseTest {

	@Test
	public void testEncodeDecode() {
		final V2NodeStats expectedNodeStats = new V2NodeStats();
		expectedNodeStats.setHttpAddress("127.0.0.1");
		expectedNodeStats.getAttributes().setData(true);

		final NodesStatsResponse expected = new NodesStatsResponse();
		expected.setClusterName("example-cluster");
		expected.getNodes().put("node1", expectedNodeStats);

		final String json = expected.toJsonString();
		final NodesStatsResponse result = JsonIterator.deserialize(json, NodesStatsResponse.class);
		Assert.assertEquals(expected, result);
	}
}
