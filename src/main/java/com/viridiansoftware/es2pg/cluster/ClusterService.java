/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.cluster;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ClusterService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterService.class);

	private static final String FALLBACK_VERSION = "5.5.0";
	private static final String FALLBACK_HASH = DigestUtils.sha1Hex(FALLBACK_VERSION);
	private static final String FALLBACK_TIMESTAMP = new DateTime().toString();

	@Autowired
	private Environment environment;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private final Map<String, Object> info = new HashMap<String, Object>();

	@PostConstruct
	public void postConstruct() {
		final Map<String, Object> versionInfo = new HashMap<String, Object>();
		versionInfo.put("build_snapshot", false);
		versionInfo.put("lucene_version", "N/A");
		versionInfo.put("postgresql_version", getDatabaseVersion());

		final URL jarUrl = ClusterService.class.getProtectionDomain().getCodeSource().getLocation();
		final String jarUrlPath = jarUrl.toString();
		if (jarUrlPath.startsWith("file:/") && jarUrlPath.endsWith(".jar")) {
			try (JarInputStream jar = new JarInputStream(jarUrl.openStream())) {
				Manifest manifest = jar.getManifest();
				versionInfo.put("number", manifest.getMainAttributes().getValue("Build-Release"));
				versionInfo.put("build_hash", manifest.getMainAttributes().getValue("Build-Hash"));
				versionInfo.put("build_timestamp", manifest.getMainAttributes().getValue("Build-Timestamp"));
			} catch (Exception e) {
			}
		}
		if (!versionInfo.containsKey("number") || !versionInfo.containsKey("build_hash")
				|| !versionInfo.containsKey("build_timestamp")) {
			versionInfo.put("number", FALLBACK_VERSION);
			versionInfo.put("build_hash", FALLBACK_HASH);
			versionInfo.put("build_timestamp", FALLBACK_TIMESTAMP);
		}

		info.put("name", environment.getRequiredProperty("es2pgsql.node.name"));
		info.put("cluster_name", environment.getRequiredProperty("es2pgsql.cluster.name"));
		info.put("cluster_uuid", environment.getRequiredProperty("es2pgsql.cluster.uuid"));
		info.put("tagline", environment.getRequiredProperty("es2pgsql.tagline"));
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
