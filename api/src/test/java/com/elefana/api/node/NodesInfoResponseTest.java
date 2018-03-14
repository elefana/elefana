/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.api.node;

import org.junit.Assert;
import org.junit.Test;

import com.elefana.api.node.v2.V2NodeInfo;
import com.jsoniter.JsonIterator;

public class NodesInfoResponseTest {

	@Test
	public void testEncodeDecode() {
		final V2NodeInfo expectedNodeInfo = new V2NodeInfo();
		expectedNodeInfo.setHttpAddress("127.0.0.1");
		expectedNodeInfo.getAttributes().setData(true);

		final NodesInfoResponse expected = new NodesInfoResponse();
		expected.setClusterName("example-cluster");
		expected.getNodes().put("node1", expectedNodeInfo);

		final String json = expected.toJsonString();
		final NodesInfoResponse result = JsonIterator.deserialize(json, NodesInfoResponse.class);
		Assert.assertEquals(expected, result);
	}
}
