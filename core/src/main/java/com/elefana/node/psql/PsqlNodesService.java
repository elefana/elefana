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
package com.elefana.node.psql;

import com.elefana.api.RequestExecutor;
import com.elefana.api.node.NodesStatsRequest;
import com.elefana.api.node.NodesStatsResponse;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.NodeStatsService;
import com.elefana.node.NodesService;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import io.netty.channel.ChannelHandlerContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;

@Service
public class PsqlNodesService implements NodesService, RequestExecutor {

	@Autowired
	Environment environment;
	@Autowired
	private NodeStatsService nodeStatsService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	private ExecutorService executorService;
	private String clusterName;

	@PostConstruct
	public void postConstruct() {
		executorService = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
				new NamedThreadFactory("elefana-nodesService-requestExecutor", ThreadPriorities.NODES_SERVICE));
		
		clusterName = environment.getRequiredProperty("elefana.cluster.name");
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();

		try {
			executorService.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {}
	}

	public NodesStatsResponse getAllNodesStats() {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats());
		return result;
	}

	public NodesStatsResponse getLocalNodeStats() {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats());
		return result;
	}
	
	public NodesStatsResponse getNodesStats(String [] filteredNodes) {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		if(nodeMatchesFilter(filteredNodes)){
			result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats());
		}
		return result;
	}

	public NodesStatsResponse getNodesStats(String [] filteredNodes, String[] infoFields) {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		if(nodeMatchesFilter(filteredNodes)){
			result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats(infoFields));
		}
		return result;
	}

	public NodesStatsResponse getAllNodesStats(String [] infoFields) {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats(infoFields));
		return result;
	}

	private boolean nodeMatchesFilter(String[] filteredNodes) {
		for (String filteredNode : filteredNodes) {
			if(nodeMatchesPattern(filteredNode))
				return true;
		}
		return false;
	}

	private boolean nodeMatchesPattern(String pattern) {
		String nodeName = nodeSettingsService.getNodeName();
		String internalId = nodeSettingsService.getNodeId();
		String address = nodeSettingsService.getHttpIp();

		if(internalId.equals(pattern))
			return true;
		if(stringMatchesWildcardPattern(nodeName, pattern))
			return true;
		if(stringMatchesWildcardPattern(address, pattern))
			return true;

		return false;
	}

	private boolean stringMatchesWildcardPattern(String inQuestion, String pattern)
	{
		return inQuestion.matches(pattern
				.replaceAll("\\*", "\\.*")
				.replaceAll("\\.", "\\\\.")
				.replaceAll("\\+", "\\\\+")
				.replaceAll("\\?", "\\\\?")
		);
	}
	
	public NodesStatsResponse getLocalNodeStats(String[] infoFields) {
		NodesStatsResponse result = new NodesStatsResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeStatsService.getNodeId(), nodeStatsService.getNodeStats(infoFields));
		return result;
	}

	@Override
	public NodesStatsRequest prepareAllNodesStats(ChannelHandlerContext context) {
		return new PsqlNodesStatsRequest(this, context);
	}

	@Override
	public NodesStatsRequest prepareNodesStats(ChannelHandlerContext context, String[] filteredNodes) {
		NodesStatsRequest result = new PsqlNodesStatsRequest(this, context);
		result.setFilteredNodes(filteredNodes);
		return result;
	}

	@Override
	public NodesStatsRequest prepareNodesStats(ChannelHandlerContext context, String[] filteredNodes, String[] infoFields) {
		NodesStatsRequest result = new PsqlNodesStatsRequest(this, context);
		result.setFilteredNodes(filteredNodes);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public NodesStatsRequest prepareAllNodesStats(ChannelHandlerContext context, String[] infoFields) {
		NodesStatsRequest result = new PsqlNodesStatsRequest(this, context);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public NodesStatsRequest prepareLocalNodeStats(ChannelHandlerContext context) {
		NodesStatsRequest result = new PsqlNodesStatsRequest(this, context);
		result.setLocalOnly(true);
		return result;
	}

	@Override
	public NodesStatsRequest prepareLocalNodeStats(ChannelHandlerContext context, String[] infoFields) {
		NodesStatsRequest result = new PsqlNodesStatsRequest(this, context);
		result.setLocalOnly(true);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
