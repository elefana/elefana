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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class NodeSettingsService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeSettingsService.class);

	private static final String DEFAULT_BRIN_PAGES_PER_RANGE = "128";

	@Autowired
	Environment environment;

	private String nodeId;
	private String nodeName;
	private String clusterId;
	private String clusterName;

	private boolean masterNode;
	private boolean dataNode;
	private boolean ingestNode;
	private String[] roles;

	private boolean httpEnabled;
	private String httpIp;
	private int httpPort;
	private String httpAddress;
	private boolean httpGzipEnabled;
	private int maxHttpPipelineEvents;
	private int maxHttpPayloadSize;
	private int httpTimeout;

	private boolean transportEnabled;
	private String transportIp;
	private int transportPort;
	private boolean transportCompressionEnabled;
	private String transportAddress;

	private boolean usingCitus = false;
	private String citusCoordinatorHost = "";
	private int citusCoordinatorPort = 5432;
	private String citusWorkerHost = "";
	private int citusWorkerPort = 5432;

	private boolean flattenJson;
	private boolean regenerateDuplicateIds;
	private int bulkParallelisation;
	private long fieldStatsInterval;
	private long mappingInterval;
	private double mappingSampleSize;
	private int fallbackMappingSampleSize;
	private long garbageCollectionInterval;
	private int brinPagesPerRange;

	@PostConstruct
	public void postConstruct() {
		nodeName = environment.getRequiredProperty("elefana.node.name");
		nodeId = DigestUtils.md5Hex(nodeName + new Random().nextInt());

		clusterName = environment.getRequiredProperty("elefana.cluster.name");
		clusterId = DigestUtils.md5Hex(clusterName);

		httpEnabled = environment.getProperty("elefana.http.enabled", Boolean.class, true);
		if (httpEnabled) {
			httpIp = environment.getRequiredProperty("elefana.http.address");
			httpPort = environment.getRequiredProperty("elefana.http.port", Integer.class);
			httpGzipEnabled = environment.getRequiredProperty("elefana.http.gzip", Boolean.class);
			maxHttpPipelineEvents = environment.getRequiredProperty("elefana.http.maxEvents", Integer.class);
			maxHttpPayloadSize = environment.getRequiredProperty("elefana.http.maxPayloadSize", Integer.class);
			httpTimeout = environment.getProperty("elefana.http.timeout", Integer.class, 300);
			httpAddress = httpIp + ":" + httpPort;
		}

		transportEnabled = environment.getProperty("elefana.transport.server.enabled", Boolean.class, false);
		if (transportEnabled) {
			transportIp = environment.getRequiredProperty("elefana.transport.server.address", String.class);
			transportPort = environment.getRequiredProperty("elefana.transport.server.port", Integer.class);
			transportCompressionEnabled = environment.getProperty("elefana.transport.server.compression", Boolean.class,
					false);
			transportAddress = transportIp + ":" + transportPort;
		}

		usingCitus = environment.getRequiredProperty("elefana.citus.enabled", Boolean.class);
		if (usingCitus) {
			citusCoordinatorHost = environment.getRequiredProperty("elefana.citus.coordinator.host");
			citusCoordinatorPort = environment.getRequiredProperty("elefana.citus.coordinator.port", Integer.class);

			citusWorkerHost = environment.getProperty("elefana.citus.worker.host", "");
			citusWorkerPort = environment.getProperty("elefana.citus.worker.port", Integer.class, 5432);
		}

		flattenJson = environment.getProperty("elefana.flattenJson", Boolean.class, false);
		bulkParallelisation = Math.max(1,
				environment.getRequiredProperty("elefana.bulkParallelisation", Integer.class));
		fieldStatsInterval = environment.getRequiredProperty("elefana.fieldStatsInterval", Long.class);
		mappingInterval = environment.getRequiredProperty("elefana.mappingInterval", Long.class);
		mappingSampleSize = environment.getRequiredProperty("elefana.mappingSampleSize", Double.class);
		fallbackMappingSampleSize = environment.getRequiredProperty("elefana.fallbackMappingSampleSize", Integer.class);
		garbageCollectionInterval = environment.getRequiredProperty("elefana.gcInterval", Long.class);
		brinPagesPerRange = Integer.parseInt(environment.getProperty("elefana.brinPagesPerRange", DEFAULT_BRIN_PAGES_PER_RANGE));
		regenerateDuplicateIds = environment.getProperty("elefana.regenerateDuplicateIds", Boolean.class, false);

		masterNode = checkIfMasterNode();
		dataNode = checkIfDataNode();

		if (!dataNode && environment.containsProperty("elefana.transport.client.hosts")) {
			String transportClientHosts = environment.getProperty("elefana.transport.client.hosts", "");
			if (!transportClientHosts.isEmpty()) {
				ingestNode = true;
			}
		}

		List<String> roles = new ArrayList<String>();
		if (masterNode) {
			roles.add("master");
		}
		if (dataNode) {
			roles.add("data");
		}
		if (ingestNode) {
			roles.add("ingest");
		}
		this.roles = new String[roles.size()];
		for (int i = 0; i < roles.size(); i++) {
			this.roles[i] = roles.get(i);
		}
		LOGGER.info("Master: " + masterNode + ", Data: " + dataNode + ", Ingest: " + ingestNode);
	}

	protected boolean checkIfMasterNode() {
		if (!usingCitus) {
			return true;
		}
		final String jdbcUrl = environment.getProperty("spring.datasource.url", "");
		if (!jdbcUrl.contains(":" + citusCoordinatorPort)) {
			return false;
		}
		if (jdbcUrl.contains(citusCoordinatorHost)) {
			return true;
		}
		if (environment.containsProperty("elefana.citus.coordinator.direct")) {
			if (environment.getRequiredProperty("elefana.citus.coordinator.direct", Boolean.class)) {
				return true;
			}
		}

		try {
			if (InetAddressValidator.getInstance().isValidInet4Address(citusCoordinatorHost)) {
				return hasMatchingInterface(citusCoordinatorHost);
			} else if (InetAddressValidator.getInstance().isValidInet6Address(citusCoordinatorHost)) {
				return hasMatchingInterface(citusCoordinatorHost);
			} else {
				try {
					InetAddress coordinatorAddress = InetAddress.getByName(citusCoordinatorHost);
					return jdbcUrl.contains(coordinatorAddress.getHostAddress())
							|| hasMatchingInterface(coordinatorAddress.getHostAddress());
				} catch (UnknownHostException e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		} catch (SocketException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	private boolean hasMatchingInterface(String ipAddress) throws SocketException {
		Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
		while (networkInterfaces.hasMoreElements()) {
			NetworkInterface networkInterface = networkInterfaces.nextElement();
			Enumeration<InetAddress> networkAddresses = networkInterface.getInetAddresses();
			while (networkAddresses.hasMoreElements()) {
				InetAddress networkAddress = networkAddresses.nextElement();
				if (networkAddress.getHostAddress().equals(ipAddress)) {
					return true;
				}
			}
		}
		return false;
	}

	protected boolean checkIfDataNode() {
		if (!environment.containsProperty("spring.datasource.url")) {
			return false;
		}
		final String jdbcUrl = environment.getProperty("spring.datasource.url", "");
		if (jdbcUrl.contains("localhost")) {
			return true;
		}
		if (jdbcUrl.contains("127.0.0.1")) {
			return true;
		}
		if (jdbcUrl.contains(httpIp)) {
			return true;
		}
		return false;
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

	public boolean isHttpEnabled() {
		return httpEnabled;
	}

	public String getHttpIp() {
		return httpIp;
	}

	public int getHttpPort() {
		return httpPort;
	}

	public String getHttpAddress() {
		return httpAddress;
	}

	public boolean isHttpGzipEnabled() {
		return httpGzipEnabled;
	}

	public int getHttpTimeout() {
		return httpTimeout;
	}

	public boolean isTransportEnabled() {
		return transportEnabled;
	}

	public String getTransportIp() {
		return transportIp;
	}

	public int getTransportPort() {
		return transportPort;
	}

	public String getTransportAddress() {
		return transportAddress;
	}

	public boolean isTransportCompressionEnabled() {
		return transportCompressionEnabled;
	}

	public int getMaxHttpPipelineEvents() {
		return maxHttpPipelineEvents;
	}

	public int getMaxHttpPayloadSize() {
		return maxHttpPayloadSize;
	}

	public boolean isUsingCitus() {
		return usingCitus;
	}

	public boolean isConnectedToCitusCoordinator() {
		if(!usingCitus) {
			return false;
		}
		return isMasterNode();
	}

	public boolean isConnectedToCitusWorker() {
		if(!usingCitus) {
			return false;
		}
		if(isMasterNode()) {
			return false;
		}
		return isDataNode();
	}

	public String getCitusCoordinatorHost() {
		return citusCoordinatorHost;
	}

	public int getCitusCoordinatorPort() {
		return citusCoordinatorPort;
	}

	public String getCitusWorkerHost() {
		return citusWorkerHost;
	}

	public int getCitusWorkerPort() {
		return citusWorkerPort;
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

	public int getBrinPagesPerRange() {
		return brinPagesPerRange;
	}

	public boolean isFlattenJson() {
		return flattenJson;
	}

	public boolean isRegenerateDuplicateIds() {
		return regenerateDuplicateIds;
	}

	public boolean isMasterNode() {
		return masterNode;
	}

	public boolean isDataNode() {
		return dataNode;
	}

	public boolean isIngestNode() {
		return ingestNode;
	}

	public String[] getRoles() {
		return roles;
	}
}
