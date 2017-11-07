/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.node;

import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.viridiansoftware.elefana.ApiVersion;

@Component
public class NodeSettingsService {
	@Autowired
	Environment environment;
	
	private String nodeId;
	private String nodeName;
	private String clusterId;
	private String clusterName;
	
	private String hostIp;
	private int port;
	private String ip;
	private String transportAddress;
	private String httpAddress;
	
	private boolean usingCitus = false;
	
	private long fieldStatsInterval;
	private long mappingInterval;
	private double mappingSampleSize;
	private long garbageCollectionInterval;
	
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
		
		usingCitus = environment.getRequiredProperty("es2pgsql.citus", Boolean.class);
		fieldStatsInterval = environment.getRequiredProperty("es2pgsql.fieldStatsInterval", Long.class);
		mappingInterval = environment.getRequiredProperty("es2pgsql.mappingInterval", Long.class);
		mappingSampleSize = environment.getRequiredProperty("es2pgsql.mappingSampleSize", Double.class);
		garbageCollectionInterval = environment.getRequiredProperty("es2pgsql.gcInterval", Long.class);
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

	public boolean isUsingCitus() {
		return usingCitus;
	}

	public long getFieldStatsInterval() {
		return fieldStatsInterval;
	}

	public long getMappingInterval() {
		return mappingInterval;
	}

	public double getMappingSampleSize() {
		return mappingSampleSize;
	}

	public long getGarbageCollectionInterval() {
		return garbageCollectionInterval;
	}
}
