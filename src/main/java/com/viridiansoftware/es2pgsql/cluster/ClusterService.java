/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.cluster;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.viridiansoftware.es2pgsql.node.NodeSettingsService;
import com.viridiansoftware.es2pgsql.node.VersionInfoService;

@Service
public class ClusterService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;

	private final Map<String, Object> info = new HashMap<String, Object>();

	@PostConstruct
	public void postConstruct() {
		final Map<String, Object> versionInfo = new HashMap<String, Object>();
		versionInfo.put("build_snapshot", false);
		versionInfo.put("lucene_version", "N/A");
		versionInfo.put("postgresql_version", getDatabaseVersion());
		versionInfo.put("number", versionInfoService.getVersionNumber());
		versionInfo.put("build_hash", versionInfoService.getBuildHash());
		versionInfo.put("build_timestamp", versionInfoService.getBuildTimestamp());

		info.put("name", nodeSettingsService.getNodeName());
		info.put("cluster_name", nodeSettingsService.getClusterName());
		info.put("cluster_uuid", nodeSettingsService.getClusterId());
		info.put("tagline", "For search or something");
		info.put("version", versionInfo);
		LOGGER.info("{}", info);
	}
	
	public String getNodeName() {
		return (String) info.get("name");
	}

	public String getClusterName() {
		return (String) info.get("cluster_name");
	}

	public Map<String, Object> getNodeRootInfo() {
		return info;
	}
	
	public Map<String, Object> getClusterHealth() {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("cluster_name", getClusterName());
		result.put("status", "green");
		result.put("timed_out", false);
		result.put("number_of_nodes", 1);
		result.put("number_of_data_nodes", 1);
		result.put("active_primary_shards", 1);
		result.put("active_shards", 1);
		result.put("relocating_shards", 0);
		result.put("initializing_shards", 0);
		result.put("unassigned_shards", 0);
		result.put("delayed_unassigned_shards", 0);
		result.put("number_of_pending_tasks", 0);
		result.put("number_of_in_flight_fetch", 0);
		result.put("task_max_waiting_in_queue_millis", 0);
		result.put("active_shards_percent_as_number", 100.0);
		return result;
	}

	private String getDatabaseVersion() {
		String result = "Unknown";
		try {
			Connection connection = jdbcTemplate.getDataSource().getConnection();
			try {
				result = connection.getMetaData().getDatabaseProductVersion();
			} catch (Exception e) {
			}
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
}
