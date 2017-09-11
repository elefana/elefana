/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.node;

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
		clusterName = environment.getRequiredProperty("es2pgsql.cluster.name");
	}

	public Map<String, Object> getNodesInfo() throws IOException {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		
		result.put("nodes", nodes);
		return result;
	}

	public Map<String, Object> getLocalNodeInfo() throws IOException {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		
		result.put("nodes", nodes);
		return result;
	}

	public Map<String, Object> getNodesInfo(String[] infoFields) throws IOException {
		// TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", clusterName);

		Map<String, Object> nodes = new HashMap<String, Object>();
		nodes.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		
		result.put("nodes", nodes);
		return result;
	}
}
