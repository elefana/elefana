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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.elefana.api.json.EmptyJsonObject;
import com.elefana.api.node.NodeInfo;
import com.elefana.api.node.v2.V2NodeInfo;
import com.elefana.api.node.v5.V5NodeInfo;
import com.elefana.node.v2.V2OsStats;
import com.elefana.node.v2.V2ProcessStats;
import com.elefana.node.v5.V5JvmStats;
import com.elefana.node.v5.V5OsStats;
import com.elefana.node.v5.V5ProcessStats;

@Service
public class NodeInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfoService.class);

	public static final String KEY_OS = "os";
	public static final String KEY_PROCESS = "process";
	public static final String KEY_JVM = "jvm";

	public static final String[] ALL_INFO = new String[] { KEY_OS, KEY_PROCESS, KEY_JVM };

	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

	@Autowired
	protected Environment environment;
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

	protected boolean masterNode;
	protected boolean dataNode;
	protected boolean ingestNode;
	protected String[] roles;

	@PostConstruct
	public void postConstruct() {
		masterNode = checkIfMasterNode();
		dataNode = checkIfDataNode();

		if (!dataNode && (nodeSettingsService.isHttpEnabled() || nodeSettingsService.isTransportEnabled())) {
			ingestNode = true;
		}

		nodeAttributes.put("master", masterNode);
		nodeAttributes.put("data", dataNode);

		httpAttributes.put("bound_address", new String[] { nodeSettingsService.getHttpIp() });
		httpAttributes.put("publish_address", nodeSettingsService.getHttpAddress());
		httpAttributes.put("profiles", EmptyJsonObject.INSTANCE);

		transportAttributes.put("bound_address", new String[] { nodeSettingsService.getHttpIp() });
		transportAttributes.put("publish_address", nodeSettingsService.getTransportAddress());
		transportAttributes.put("profiles", EmptyJsonObject.INSTANCE);

		switch (versionInfoService.getApiVersion()) {
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

		scheduledExecutorService.scheduleAtFixedRate(jvmStats, 0L, 1L, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(osStats, 0L, 1L, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(processStats, 0L, 1L, TimeUnit.SECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		scheduledExecutorService.shutdown();
	}

	public NodeInfo getNodeInfo(String... infoFields) {
		NodeInfo result = null;

		switch (versionInfoService.getApiVersion()) {
		case V_5_5_2: {
			V5NodeInfo v5NodeInfo = new V5NodeInfo();

			v5NodeInfo.setRoles(new String[] { (dataNode ? "data" : "") });

			v5NodeInfo.getHttp().setBoundAddress(new String[] { nodeSettingsService.getHttpIp() });
			v5NodeInfo.getHttp().setPublishAddress(nodeSettingsService.getHttpAddress());

			v5NodeInfo.getTransport().setBoundAddress(new String[] { nodeSettingsService.getTransportIp() });
			v5NodeInfo.getTransport().setPublishAddress(nodeSettingsService.getTransportAddress());

			result = v5NodeInfo;
			break;
		}
		default:
		case V_2_4_3: {
			V2NodeInfo v2NodeInfo = new V2NodeInfo();
			v2NodeInfo.setHttpAddress(nodeSettingsService.getHttpAddress());
			v2NodeInfo.getAttributes().setData(checkIfDataNode());

			result = v2NodeInfo;
			break;
		}
		}

		result.setName(nodeSettingsService.getNodeName());
		result.setTransportAddress(nodeSettingsService.getTransportAddress());
		result.setHost(nodeSettingsService.getHttpIp());
		result.setIp(nodeSettingsService.getHttpIp());
		result.setVersion(versionInfoService.getVersionNumber());
		result.setBuild(versionInfoService.getBuildHash());

		for (int i = 0; i < infoFields.length; i++) {
			if (infoFields[i] == null) {
				continue;
			}
			if (infoFields[i].isEmpty()) {
				continue;
			}
			switch (infoFields[i]) {
			case KEY_OS:
				result.setOs(osStats.getCurrentStats());
				break;
			case KEY_JVM:
				result.setJvm(jvmStats.getCurrentStats());
				break;
			case KEY_PROCESS:
				result.setProcess(processStats.getCurrentStats());
				break;
			default:
				continue;
			}
		}
		return result;
	}

	public NodeInfo getNodeInfo() {
		return getNodeInfo(ALL_INFO);
	}

	public String getNodeId() {
		return nodeSettingsService.getNodeId();
	}

	public String getNodeName() {
		return nodeSettingsService.getNodeName();
	}

	protected boolean checkIfMasterNode() {
		if (!nodeSettingsService.isUsingCitus()) {
			return true;
		}
		final String jdbcUrl = environment.getProperty("spring.datasource.url", "");
		if (jdbcUrl.contains(nodeSettingsService.getCitusCoordinatorHost())) {
			return true;
		}
		if (environment.containsProperty("elefana.citus.coordinator.direct")) {
			if (environment.getRequiredProperty("elefana.citus.coordinator.direct", Boolean.class)) {
				return true;
			}
		}

		try {
			if (InetAddressValidator.getInstance().isValidInet4Address(nodeSettingsService.getCitusCoordinatorHost())) {
				return hasMatchingInterface(nodeSettingsService.getCitusCoordinatorHost());
			} else if (InetAddressValidator.getInstance()
					.isValidInet6Address(nodeSettingsService.getCitusCoordinatorHost())) {
				return hasMatchingInterface(nodeSettingsService.getCitusCoordinatorHost());
			} else {
				try {
					InetAddress coordinatorAddress = InetAddress
							.getByName(nodeSettingsService.getCitusCoordinatorHost());
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
		final String jdbcUrl = environment.getProperty("spring.datasource.url", "");
		if (jdbcUrl.contains("localhost")) {
			return true;
		}
		if (jdbcUrl.contains("127.0.0.1")) {
			return true;
		}
		if (jdbcUrl.contains(nodeSettingsService.getHttpIp())) {
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

	public OsStats getOsStats() {
		return osStats;
	}

	public JvmStats getJvmStats() {
		return jvmStats;
	}

	public ProcessStats getProcessStats() {
		return processStats;
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
}
