/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

import com.elefana.api.json.EmptyJsonObject;
import com.elefana.api.node.NodeStats;
import com.elefana.api.node.v2.V2NodeStats;
import com.elefana.api.node.v5.V5NodeStats;
import com.elefana.node.v2.V2JvmStats;
import com.elefana.node.v2.V2OsStats;
import com.elefana.node.v2.V2ProcessStats;
import com.elefana.node.v5.V5JvmStats;
import com.elefana.node.v5.V5OsStats;
import com.elefana.node.v5.V5ProcessStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service("nodeStatsService")
@DependsOn("nodeSettingsService")
public class CoreNodeStatsService implements NodeStatsService {
	private static final Logger LOGGER = LoggerFactory.getLogger(CoreNodeStatsService.class);

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

	@PostConstruct
	public void postConstruct() {
		nodeAttributes.put("master", nodeSettingsService.isMasterNode());
		nodeAttributes.put("data", nodeSettingsService.isDataNode());

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
			jvmStats = new V2JvmStats();
			break;
		}

		long refreshInterval = environment.getProperty("elefana.service.node.statsRefreshInterval", Long.class, 1L);

		scheduledExecutorService.scheduleAtFixedRate(jvmStats, 0L, refreshInterval, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(osStats, 0L, refreshInterval, TimeUnit.SECONDS);
		scheduledExecutorService.scheduleAtFixedRate(processStats, 0L, refreshInterval, TimeUnit.SECONDS);
	}

	@PreDestroy
	public void preDestroy() {
		scheduledExecutorService.shutdown();

		try {
			scheduledExecutorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}

	public NodeStats getNodeStats(String... infoFields) {
		NodeStats result = null;

		switch (versionInfoService.getApiVersion()) {
			case V_5_5_2: {
				V5NodeStats v5NodeStats = new V5NodeStats();

				v5NodeStats.setRoles(nodeSettingsService.getRoles());

				v5NodeStats.getHttp().setBoundAddress(new String[] { nodeSettingsService.getHttpIp() });
				v5NodeStats.getHttp().setPublishAddress(nodeSettingsService.getHttpAddress());

				v5NodeStats.getTransport().setBoundAddress(new String[] { nodeSettingsService.getTransportIp() });
				v5NodeStats.getTransport().setPublishAddress(nodeSettingsService.getTransportAddress());

				result = v5NodeStats;
				break;
			}
			default:
			case V_2_4_3: {
				V2NodeStats v2NodeStats = new V2NodeStats();
				v2NodeStats.setHttpAddress(nodeSettingsService.getHttpAddress());
				v2NodeStats.getAttributes().setData(nodeSettingsService.isDataNode());

				result = v2NodeStats;
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

	public NodeStats getNodeStats() {
		return getNodeStats(ALL_INFO);
	}

	public String getNodeId() {
		return nodeSettingsService.getNodeId();
	}

	public String getNodeName() {
		return nodeSettingsService.getNodeName();
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
		return nodeSettingsService.isMasterNode();
	}

	public boolean isDataNode() {
		return nodeSettingsService.isDataNode();
	}

	public boolean isIngestNode() {
		return nodeSettingsService.isIngestNode();
	}
}
