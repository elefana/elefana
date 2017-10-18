/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.viridiansoftware.es2pgsql.util.EmptyJsonObject;

public abstract class NodeInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfoService.class);
	
	public static final String KEY_OS = "os";
	public static final String KEY_PROCESS = "process";
	public static final String KEY_JVM = "jvm";

	public static final String[] ALL_INFO = new String[] { KEY_OS, KEY_PROCESS, KEY_JVM };
	
	@Autowired
	Environment environment;
	@Autowired
	protected NodeSettingsService nodeSettingsService;
	@Autowired
	protected VersionInfoService versionInfoService;
	
	protected final Map<String, Object> nodeAttributes = new HashMap<String, Object>();
	protected final Map<String, Object> httpAttributes = new HashMap<String, Object>();
	protected final Map<String, Object> transportAttributes = new HashMap<String, Object>();
	
	protected boolean dataNode;
	
	@PostConstruct
	public void postConstruct() {
		LOGGER.info(System.getProperty("java.io.tmpdir"));
		
		initialiseEsServices();
		
		dataNode = checkIfDataNode();
		nodeAttributes.put("data", dataNode);
		nodeAttributes.put("master", false);
		
		httpAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		httpAttributes.put("publish_address", nodeSettingsService.getHttpAddress());
		httpAttributes.put("profiles", EmptyJsonObject.INSTANCE);
		
		transportAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		transportAttributes.put("publish_address", nodeSettingsService.getTransportAddress());
		transportAttributes.put("profiles", EmptyJsonObject.INSTANCE);
	}
	
	protected abstract void initialiseEsServices();

	public abstract Map<String, Object> getNodeInfo(String... infoFields) throws IOException;
	
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
}
