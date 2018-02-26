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

import com.elefana.api.document.BulkResponse;
import com.elefana.api.node.v2.V2NodeInfo;
import com.elefana.api.node.v5.V5HttpAttributes;
import com.elefana.api.node.v5.V5NodeInfo;
import com.elefana.api.node.v5.V5TransportAttributes;
import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;

public class NodeInfoTest {

	@Test
	public void testV2NodeInfoSerialization() {
		final V2NodeInfo expectedNodeInfo = new V2NodeInfo();
		expectedNodeInfo.setHttpAddress("127.0.0.1");
		expectedNodeInfo.getAttributes().setData(true);
		
		String json = JsonStream.serialize(expectedNodeInfo);
		NodeInfo result = JsonIterator.deserialize(json, NodeInfo.class);
		Assert.assertEquals(true, result instanceof V2NodeInfo);
		
		V2NodeInfo v2Result = (V2NodeInfo) result;
		Assert.assertEquals(true, v2Result.getAttributes().isData());
		Assert.assertEquals(expectedNodeInfo, result);
	}
	
	@Test
	public void testV5NodeInfoSerialization() {
		final V5NodeInfo expectedNodeInfo = new V5NodeInfo();
		expectedNodeInfo.setHttp(new V5HttpAttributes());
		expectedNodeInfo.getHttp().setPublishAddress("127.0.0.1");
		expectedNodeInfo.setTransport(new V5TransportAttributes());
		expectedNodeInfo.getTransport().setPublishAddress("127.0.0.1");
		
		String json = JsonStream.serialize(expectedNodeInfo);
		NodeInfo result = JsonIterator.deserialize(json, NodeInfo.class);
		Assert.assertEquals(expectedNodeInfo, result);
	}
}
