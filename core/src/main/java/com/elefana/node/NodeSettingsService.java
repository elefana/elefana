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
package com.elefana.node;

import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

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
	
	private int bulkParallelisation;
	private long fieldStatsInterval;
	private long mappingInterval;
	private double mappingSampleSize;
	private int fallbackMappingSampleSize;
	private long garbageCollectionInterval;
	
	@PostConstruct
	public void postConstruct() {
		nodeName = environment.getRequiredProperty("elefana.node.name");
		nodeId = DigestUtils.md5Hex(nodeName + new Random().nextInt());
		
		clusterName = environment.getRequiredProperty("elefana.cluster.name");
		clusterId = DigestUtils.md5Hex(clusterName);
		
		hostIp = environment.getRequiredProperty("server.address");
		port = environment.getRequiredProperty("server.port", Integer.class);
		
		ip = hostIp;
		transportAddress = hostIp + ":9300";
		httpAddress = hostIp + port;
		
		usingCitus = environment.getRequiredProperty("elefana.citus", Boolean.class);
		
		bulkParallelisation = Math.max(1, environment.getRequiredProperty("elefana.bulkParallelisation", Integer.class));
		fieldStatsInterval = environment.getRequiredProperty("elefana.fieldStatsInterval", Long.class);
		mappingInterval = environment.getRequiredProperty("elefana.mappingInterval", Long.class);
		mappingSampleSize = environment.getRequiredProperty("elefana.mappingSampleSize", Double.class);
		fallbackMappingSampleSize = environment.getRequiredProperty("elefana.fallbackMappingSampleSize", Integer.class);
		garbageCollectionInterval = environment.getRequiredProperty("elefana.gcInterval", Long.class);
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

	public int getBulkParallelisation() {
		return bulkParallelisation;
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

	public int getFallbackMappingSampleSize() {
		return fallbackMappingSampleSize;
	}

	public long getGarbageCollectionInterval() {
		return garbageCollectionInterval;
	}
}
