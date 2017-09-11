/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.env.Environment;

import com.viridiansoftware.es2pgsql.node.NodeInfoService;

public class NodeInfoServiceTest {
	private final NodeInfoService nodeInfoService = new NodeInfoService();
	
	private Environment environment;
	
	@Before
	public void setUp() {
		environment = mock(Environment.class);
		when(environment.getRequiredProperty("es2pgsql.node.name")).thenReturn("test");
		
		nodeInfoService.environment = environment;
		nodeInfoService.postConstruct();
	}
	
	@Test
	public void testJvmStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_JVM);
		Assert.assertEquals(false, result.isEmpty());
		System.out.println(Arrays.toString(result.entrySet().toArray()));
	}
	
	@Test
	public void testOsStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_OS);
		Assert.assertEquals(false, result.isEmpty());
		System.out.println(Arrays.toString(result.entrySet().toArray()));
	}
	
	@Test
	public void testProcessStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_PROCESS);
		Assert.assertEquals(false, result.isEmpty());
		System.out.println(Arrays.toString(result.entrySet().toArray()));
	}
}
