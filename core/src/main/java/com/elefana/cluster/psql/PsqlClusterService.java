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
package com.elefana.cluster.psql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.elefana.api.RequestExecutor;
import com.elefana.api.cluster.ClusterHealthRequest;
import com.elefana.api.cluster.ClusterHealthResponse;
import com.elefana.api.cluster.ClusterInfoRequest;
import com.elefana.api.cluster.ClusterInfoResponse;
import com.elefana.api.cluster.ClusterSettingsRequest;
import com.elefana.api.cluster.ClusterSettingsResponse;
import com.elefana.cluster.ClusterService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;

@Service
public class PsqlClusterService implements ClusterService, RequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PsqlClusterService.class);
	private static final String TAGLINE = "For search or something";

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;

	private ExecutorService executorService;
	private final ClusterInfoResponse clusterInfoResponse = new ClusterInfoResponse();
	private final Map<String, Object> info = new HashMap<String, Object>();

	@PostConstruct
	public void postConstruct() {
		executorService = Executors.newSingleThreadExecutor();
		
		clusterInfoResponse.getVersion().setBuildSnapshot(false);
		clusterInfoResponse.getVersion().setLuceneVersion("N/A");
		clusterInfoResponse.getVersion().setPostgresqlVersion(getDatabaseVersion());
		clusterInfoResponse.getVersion().setNumber(versionInfoService.getVersionNumber());
		clusterInfoResponse.getVersion().setBuildHash(versionInfoService.getBuildHash());
		clusterInfoResponse.getVersion().setBuildTimestamp(versionInfoService.getBuildTimestamp());
		
		clusterInfoResponse.setName(nodeSettingsService.getNodeName());
		clusterInfoResponse.setClusterName(nodeSettingsService.getClusterName());
		clusterInfoResponse.setClusterUuid(nodeSettingsService.getClusterId());
		clusterInfoResponse.setTagline(TAGLINE);
	}
	
	public String getNodeName() {
		return (String) info.get("name");
	}

	public String getClusterName() {
		return (String) info.get("cluster_name");
	}

	public ClusterInfoResponse getClusterInfo() {
		return clusterInfoResponse;
	}
	
	public ClusterHealthResponse getClusterHealth() {
		ClusterHealthResponse result = new ClusterHealthResponse();
		result.setClusterName(getClusterName());
		result.setStatus("green");
		result.setTimedOut(false);
		result.setNumberOfNodes(1);
		result.setNumberOfDataNodes(1);
		result.setActivePrimaryShards(1);
		result.setActiveShards(1);
		result.setRelocatingShards(0);
		result.setInitializingShards(0);
		result.setUnassignedShards(0);
		result.setDelayedUnassignedShards(0);
		result.setNumberOfPendingTasks(0);
		result.setNumberOfInFlightFetch(0);
		result.setTaskMaxWaitingInQueueMillis(0L);
		result.setActiveShardsPercentAsNumber(100.0);
		return result;
	}
	
	public ClusterSettingsResponse getClusterSettings() {
		return new ClusterSettingsResponse();
	}

	private String getDatabaseVersion() {
		String result = "Unknown";
		Connection connection = null;
		try {
			connection = jdbcTemplate.getDataSource().getConnection();
			result = connection.getMetaData().getDatabaseProductVersion();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if(connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public ClusterInfoRequest prepareClusterInfo() {
		return new PsqlClusterInfoRequest(this);
	}

	@Override
	public ClusterHealthRequest prepareClusterHealth() {
		return new PsqlClusterHealthRequest(this);
	}

	@Override
	public ClusterSettingsRequest prepareClusterSettings() {
		return new PsqlClusterSettingsRequest(this);
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
