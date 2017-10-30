/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.env.Environment;

import com.viridiansoftware.es2pgsql.ApiVersion;

public class NodeInfoServiceTest {
	private final NodeInfoService nodeInfoService = new NodeInfoService();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
	
	private Environment environment;
	private NodeSettingsService nodeSettingsService;
	private VersionInfoService versionInfoService;
	
	@Before
	public void setUp() {
		environment = mock(Environment.class);
		nodeSettingsService = mock(NodeSettingsService.class);
		versionInfoService = mock(VersionInfoService.class);
		
		when(environment.getRequiredProperty("es2pgsql.node.name")).thenReturn("test");
		when(environment.getRequiredProperty("spring.datasource.url")).thenReturn("localhost");
		when(versionInfoService.getApiVersion()).thenReturn(ApiVersion.V_5_5_2);
		
		nodeInfoService.environment = environment;
		nodeInfoService.scheduledExecutorService = scheduledExecutorService;
		nodeInfoService.nodeSettingsService = nodeSettingsService;
		nodeInfoService.versionInfoService = versionInfoService;
		nodeInfoService.postConstruct();
		
		try {
			Thread.sleep(1000);
		} catch (Exception e) {}
	}
	
	@After
	public void teardown() {
		scheduledExecutorService.shutdownNow();
	}
	
	@Test
	public void testJvmStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_JVM);
		Assert.assertEquals(false, result.isEmpty());
	}
	
	@Test
	public void testOsStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_OS);
		Assert.assertEquals(false, result.isEmpty());
	}
	
	@Test
	public void testProcessStats() throws IOException {
		Map<String, Object> result = nodeInfoService.getNodeInfo(NodeInfoService.KEY_PROCESS);
		Assert.assertEquals(false, result.isEmpty());
	}
}
