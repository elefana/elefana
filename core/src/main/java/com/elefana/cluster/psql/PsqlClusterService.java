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

import com.elefana.api.RequestExecutor;
import com.elefana.api.cluster.*;
import com.elefana.cluster.ClusterService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@Service
@DependsOn("nodeSettingsService")
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
		executorService = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors() / 2), new NamedThreadFactory(
				"elefana-clusterService-requestExecutor", ThreadPriorities.CLUSTER_SERVICE));
		
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

	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();

		try {
			executorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
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
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public ClusterInfoRequest prepareClusterInfo(ChannelHandlerContext context) {
		return new PsqlClusterInfoRequest(this, context);
	}

	@Override
	public ClusterHealthRequest prepareClusterHealth(ChannelHandlerContext context) {
		return new PsqlClusterHealthRequest(this, context);
	}
	
	@Override
	public ClusterHealthRequest prepareClusterHealth(ChannelHandlerContext context, String indices) {
		return new PsqlClusterHealthRequest(this, context, indices);
	}

	@Override
	public ClusterHealthRequest prepareClusterHealth(ChannelHandlerContext context, String... indices) {
		return new PsqlClusterHealthRequest(this, context, indices);
	}

	@Override
	public ClusterSettingsRequest prepareClusterSettings(ChannelHandlerContext context) {
		return new PsqlClusterSettingsRequest(this, context);
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
