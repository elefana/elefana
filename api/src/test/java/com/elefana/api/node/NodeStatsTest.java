/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.api.node;

import org.junit.Assert;
import org.junit.Test;

import com.elefana.api.node.v2.V2NodeStats;
import com.elefana.api.node.v5.V5HttpAttributes;
import com.elefana.api.node.v5.V5NodeStats;
import com.elefana.api.node.v5.V5TransportAttributes;
import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;

public class NodeStatsTest {

	@Test
	public void testV2NodeStatsSerialization() {
		final V2NodeStats expectedNodeStats = new V2NodeStats();
		expectedNodeStats.setHttpAddress("127.0.0.1");
		expectedNodeStats.getAttributes().setData(true);
		
		String json = JsonStream.serialize(expectedNodeStats);
		NodeStats result = JsonIterator.deserialize(json, NodeStats.class);
		Assert.assertEquals(true, result instanceof V2NodeStats);
		
		V2NodeStats v2Result = (V2NodeStats) result;
		Assert.assertEquals(true, v2Result.getAttributes().isData());
		Assert.assertEquals(expectedNodeStats, result);
	}
	
	@Test
	public void testV5NodeStatsSerialization() {
		final V5NodeStats expectedNodeStats = new V5NodeStats();
		expectedNodeStats.setHttp(new V5HttpAttributes());
		expectedNodeStats.getHttp().setPublishAddress("127.0.0.1");
		expectedNodeStats.setTransport(new V5TransportAttributes());
		expectedNodeStats.getTransport().setPublishAddress("127.0.0.1");
		
		String json = JsonStream.serialize(expectedNodeStats);
		NodeStats result = JsonIterator.deserialize(json, NodeStats.class);
		Assert.assertEquals(expectedNodeStats, result);
	}
}
