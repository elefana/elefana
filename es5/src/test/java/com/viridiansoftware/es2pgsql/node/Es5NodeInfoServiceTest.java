/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.env.Environment;

import com.viridiansoftware.es2pgsql.ApiVersion;
import com.viridiansoftware.es2pgsql.es5.node.Es5NodeInfoService;

public class Es5NodeInfoServiceTest {
	private final NodeInfoService nodeInfoService = new Es5NodeInfoService();
	
	private Environment environment;
	private NodeSettingsService nodeSettingsService;
	private VersionInfoService versionInfoService;
	
	@Before
	public void setUp() {
		environment = mock(Environment.class);
		when(environment.getRequiredProperty("es2pgsql.node.name")).thenReturn("test");
		when(environment.getRequiredProperty("spring.datasource.url")).thenReturn("localhost");
		
		nodeSettingsService = mock(NodeSettingsService.class);
		when(nodeSettingsService.getApiVersion()).thenReturn(ApiVersion.V_5_5_2);
		
		versionInfoService = mock(VersionInfoService.class);
		
		nodeInfoService.environment = environment;
		nodeInfoService.nodeSettingsService = nodeSettingsService;
		nodeInfoService.versionInfoService = versionInfoService;
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
