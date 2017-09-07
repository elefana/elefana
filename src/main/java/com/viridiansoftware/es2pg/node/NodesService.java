/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class NodesService {
	@Autowired
	private NodeInfoService nodeInfoService;

	public Map<String, Object> getNodesInfo() throws IOException {
		//TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo());
		return result;
	}
	
	public Map<String, Object> getNodesInfo(String [] infoFields) throws IOException {
		//TODO: Support clustering
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(nodeInfoService.getNodeId(), nodeInfoService.getNodeInfo(infoFields));
		return result;
	}
}
