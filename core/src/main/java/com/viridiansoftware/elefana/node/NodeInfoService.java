/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.viridiansoftware.elefana.node.v2.V2OsStats;
import com.viridiansoftware.elefana.node.v2.V2ProcessStats;
import com.viridiansoftware.elefana.node.v5.V5JvmStats;
import com.viridiansoftware.elefana.node.v5.V5OsStats;
import com.viridiansoftware.elefana.node.v5.V5ProcessStats;
import com.viridiansoftware.elefana.util.EmptyJsonObject;

@Service
public class NodeInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfoService.class);
	
	public static final String KEY_OS = "os";
	public static final String KEY_PROCESS = "process";
	public static final String KEY_JVM = "jvm";

	public static final String[] ALL_INFO = new String[] { KEY_OS, KEY_PROCESS, KEY_JVM };
	
	@Autowired
	protected Environment environment;
	@Autowired
	protected ScheduledExecutorService scheduledExecutorService;
	@Autowired
	protected NodeSettingsService nodeSettingsService;
	@Autowired
	protected VersionInfoService versionInfoService;
	
	protected final Map<String, Object> nodeAttributes = new HashMap<String, Object>();
	protected final Map<String, Object> httpAttributes = new HashMap<String, Object>();
	protected final Map<String, Object> transportAttributes = new HashMap<String, Object>();
	
	protected OsStats osStats;
	protected JvmStats jvmStats;
	protected ProcessStats processStats;
	
	protected boolean dataNode;
	
	@PostConstruct
	public void postConstruct() {
		dataNode = checkIfDataNode();
		nodeAttributes.put("data", dataNode);
		nodeAttributes.put("master", false);
		
		httpAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		httpAttributes.put("publish_address", nodeSettingsService.getHttpAddress());
		httpAttributes.put("profiles", EmptyJsonObject.INSTANCE);
		
		transportAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		transportAttributes.put("publish_address", nodeSettingsService.getTransportAddress());
		transportAttributes.put("profiles", EmptyJsonObject.INSTANCE);
		
		switch(versionInfoService.getApiVersion()) {
		case V_5_5_2:
			osStats = new V5OsStats();
			processStats = new V5ProcessStats();
			jvmStats = new V5JvmStats();
			break;
		case V_2_4_3:
		default:
			osStats = new V2OsStats();
			processStats = new V2ProcessStats();
			jvmStats = new V5JvmStats();
			break;
		}
		
		scheduledExecutorService.scheduleAtFixedRate(jvmStats, 0L, 1L, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(osStats, 0L, 1L, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(processStats, 0L, 1L, TimeUnit.SECONDS);
	}

	public Map<String, Object> getNodeInfo(String... infoFields) throws IOException {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("name", nodeSettingsService.getNodeName());
		result.put("transport_address", nodeSettingsService.getTransportAddress());
		result.put("host", nodeSettingsService.getHostIp());
		result.put("ip", nodeSettingsService.getIp());
		result.put("version", versionInfoService.getVersionNumber());
		result.put("build", versionInfoService.getBuildHash());
		
		switch(versionInfoService.getApiVersion()) {
		case V_2_4_3:
			result.put("http_address", nodeSettingsService.getHttpAddress());
			result.put("attributes", nodeAttributes);
			break;
		case V_5_5_2:
		default:
			result.put("http", httpAttributes);
			result.put("transport", transportAttributes);
			result.put("roles", new String [] { (dataNode ? "data" : "") });
			break;
		}
		
		for (int i = 0; i < infoFields.length; i++) {
			if(infoFields[i] == null) {
				continue;
			}
			if(infoFields[i].isEmpty()) {
				continue;
			}
			switch (infoFields[i]) {
			case KEY_OS:
				result.put("os", osStats.getCurrentStats());
				break;
			case KEY_JVM:
				result.put("jvm", jvmStats.getCurrentStats());
				break;
			case KEY_PROCESS:
				result.put("process", jvmStats.getCurrentStats());
				break;
			default:
				continue;
			}
		}
		return result;
	}
	
	public Map<String, Object> getNodeInfo() throws IOException {
		return getNodeInfo(ALL_INFO);
	}

	public String getNodeId() {
		return nodeSettingsService.getNodeId();
	}

	public String getNodeName() {
		return nodeSettingsService.getNodeName();
	}
	
	protected boolean checkIfDataNode() {
		String jdbcUrl = environment.getRequiredProperty("spring.datasource.url");
		if(jdbcUrl.contains("localhost")) {
			return true;
		}
		if(jdbcUrl.contains("127.0.0.1")) {
			return true;
		}
		if(jdbcUrl.contains(nodeSettingsService.getHostIp())) {
			return true;
		}
		return false;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public void setNodeSettingsService(NodeSettingsService nodeSettingsService) {
		this.nodeSettingsService = nodeSettingsService;
	}

	public void setVersionInfoService(VersionInfoService versionInfoService) {
		this.versionInfoService = versionInfoService;
	}
}
