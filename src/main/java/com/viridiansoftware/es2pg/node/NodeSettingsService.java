/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.node;

import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.viridiansoftware.es2pg.ApiVersion;

@Component
public class NodeSettingsService {
	@Autowired
	Environment environment;
	
	private ApiVersion apiVersion = ApiVersion.V_5_5_2;
	
	private String nodeId;
	private String nodeName;
	private String clusterId;
	private String clusterName;
	
	private String hostIp;
	private int port;
	private String ip;
	private String transportAddress;
	private String httpAddress;
	
	@PostConstruct
	public void postConstruct() {
		nodeName = environment.getRequiredProperty("es2pgsql.node.name");
		nodeId = DigestUtils.md5Hex(nodeName + new Random().nextInt());
		
		clusterName = environment.getRequiredProperty("es2pgsql.cluster.name");
		clusterId = DigestUtils.md5Hex(clusterName);
		
		hostIp = environment.getRequiredProperty("server.address");
		port = environment.getRequiredProperty("server.port", Integer.class);
		
		ip = hostIp;
		transportAddress = hostIp + ":9300";
		httpAddress = hostIp + port;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public String getNodeId() {
		return nodeId;
	}

	public String getNodeName() {
		return nodeName;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getHostIp() {
		return hostIp;
	}

	public int getPort() {
		return port;
	}

	public String getIp() {
		return ip;
	}

	public String getTransportAddress() {
		return transportAddress;
	}

	public String getHttpAddress() {
		return httpAddress;
	}

	public ApiVersion getApiVersion() {
		return apiVersion;
	}
}
