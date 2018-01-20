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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
public class NodesService {
	@Autowired
	Environment environment;
	@Autowired
	private NodeInfoService nodeInfoService;

	private String clusterName;

	@PostConstruct
	public void postConstruct() {
		clusterName = environment.getRequiredProperty("elefana.cluster.name");
	}

	public Map<String, Object> getNodesInfo() {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		
		result.put("nodes", nodes);
		return result;
	}

	public Map<String, Object> getLocalNodeInfo() {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		
		result.put("nodes", nodes);
		return result;
	}
	
	public Map<String, Object> getNodesInfo(String [] filteredNodes) {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		
		result.put("nodes", nodes);
		return result;
	}

	public Map<String, Object> getNodesInfo(String [] filteredNodes, String[] infoFields) {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		
		result.put("nodes", nodes);
		return result;
	}
	
	public Map<String, Object> getLocalNodeInfo(String[] infoFields) {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		
		result.put("nodes", nodes);
		return result;
	}
}
