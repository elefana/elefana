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
import com.elefana.api.node.NodesInfoRequest;
import com.elefana.api.node.NodesInfoResponse;
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.NodesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class PsqlNodesService implements NodesService, RequestExecutor {

	@Autowired
	Environment environment;
	@Autowired
	private NodeInfoService nodeInfoService;
	@Autowired
	private NodeSettingsService nodeSettingsService;

	private ExecutorService executorService;
	private String clusterName;

	@PostConstruct
	public void postConstruct() {
		final int totalThreads = environment.getProperty("elefana.service.node.threads", Integer.class, 2);
		executorService = Executors.newFixedThreadPool(totalThreads);
		
		clusterName = environment.getRequiredProperty("elefana.cluster.name");
	}
	
	@PreDestroy
	public void preDestroy() {
		executorService.shutdown();
	}

	public NodesInfoResponse getAllNodesInfo() {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		return result;
	}

	public NodesInfoResponse getLocalNodeInfo() {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		return result;
	}
	
	public NodesInfoResponse getNodesInfo(String [] filteredNodes) {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		if(nodeMatchesFilter(filteredNodes)){
			result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		}
		return result;
	}

	public NodesInfoResponse getNodesInfo(String [] filteredNodes, String[] infoFields) {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		if(nodeMatchesFilter(filteredNodes)){
			result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		}
		return result;
	}

	public NodesInfoResponse getAllNodesInfo(String [] infoFields) {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
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
	
	public NodesInfoResponse getLocalNodeInfo(String[] infoFields) {
		NodesInfoResponse result = new NodesInfoResponse();
		result.setClusterName(clusterName);
		result.getNodes().put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		return result;
	}

	@Override
	public NodesInfoRequest prepareAllNodesInfo() {
		return new PsqlNodesInfoRequest(this);
	}

	@Override
	public NodesInfoRequest prepareNodesInfo(String[] filteredNodes) {
		NodesInfoRequest result = new PsqlNodesInfoRequest(this);
		result.setFilteredNodes(filteredNodes);
		return result;
	}

	@Override
	public NodesInfoRequest prepareNodesInfo(String[] filteredNodes, String[] infoFields) {
		NodesInfoRequest result = new PsqlNodesInfoRequest(this);
		result.setFilteredNodes(filteredNodes);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public NodesInfoRequest prepareAllNodesInfo(String[] infoFields) {
		NodesInfoRequest result = new PsqlNodesInfoRequest(this);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public NodesInfoRequest prepareLocalNodeInfo() {
		NodesInfoRequest result = new PsqlNodesInfoRequest(this);
		result.setLocalOnly(true);
		return result;
	}

	@Override
	public NodesInfoRequest prepareLocalNodeInfo(String[] infoFields) {
		NodesInfoRequest result = new PsqlNodesInfoRequest(this);
		result.setLocalOnly(true);
		result.setInfoFields(infoFields);
		return result;
	}

	@Override
	public <T> Future<T> submit(Callable<T> request) {
		return executorService.submit(request);
	}
}
