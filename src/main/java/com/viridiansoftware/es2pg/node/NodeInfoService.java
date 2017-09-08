/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.viridiansoftware.es2pg.util.EmptyJsonObject;
import com.viridiansoftware.es2pg.util.MapXContentBuilder;

@Service
public class NodeInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfoService.class);
	
	public static final String KEY_OS = "os";
	public static final String KEY_PROCESS = "process";
	public static final String KEY_JVM = "jvm";

	private static final String[] ALL_INFO = new String[] { KEY_OS, KEY_PROCESS, KEY_JVM };
	
	@Autowired
	Environment environment;
	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private VersionInfoService versionInfoService;

	private OsService osService;
	private ProcessService processService;
	private JvmService jvmService;
	// private final FsService fsService;
	
	private final Map<String, Object> nodeAttributes = new HashMap<String, Object>();
	private final Map<String, Object> httpAttributes = new HashMap<String, Object>();
	private final Map<String, Object> transportAttributes = new HashMap<String, Object>();
	
	private boolean dataNode;
	
	@PostConstruct
	public void postConstruct() {
		LOGGER.info(System.getProperty("java.io.tmpdir"));
		
		final Settings settings = Settings.builder().put("node.name", nodeSettingsService.getNodeName()).build();
		final ThreadPool threadPool = new ThreadPool(settings, new ExecutorBuilder[0]);

		osService = new OsService(settings);
		processService = new ProcessService(settings);
		jvmService = new JvmService(settings);
		
		dataNode = checkIfDataNode();
		nodeAttributes.put("data", dataNode);
		nodeAttributes.put("master", false);
		
		httpAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		httpAttributes.put("publish_address", nodeSettingsService.getHttpAddress());
		httpAttributes.put("profiles", EmptyJsonObject.INSTANCE);
		
		transportAttributes.put("bound_address", new String [] { nodeSettingsService.getHostIp() });
		transportAttributes.put("publish_address", nodeSettingsService.getTransportAddress());
		transportAttributes.put("profiles", EmptyJsonObject.INSTANCE);
	}

	public Map<String, Object> getNodeInfo() throws IOException {
		return getNodeInfo(ALL_INFO);
	}

	public Map<String, Object> getNodeInfo(String... infoFields) throws IOException {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("name", nodeSettingsService.getNodeName());
		result.put("transport_address", nodeSettingsService.getTransportAddress());
		result.put("host", nodeSettingsService.getHostIp());
		result.put("ip", nodeSettingsService.getIp());
		result.put("version", versionInfoService.getVersionNumber());
		result.put("build", versionInfoService.getBuildHash());
		
		switch(nodeSettingsService.getApiVersion()) {
		case V_2_4_3:
			result.put("http_address", nodeSettingsService.getHttpAddress());
			result.put("attributes", nodeAttributes);
			break;
		case V_5_5_2:
		default:
			result.put("http", httpAttributes);
			result.put("transport", transportAttributes);
			result.put("roles", new String [] { (dataNode ? "data" : "") });
			break;
		}
		
		for (int i = 0; i < infoFields.length; i++) {
			if(infoFields[i] == null) {
				continue;
			}
			if(infoFields[i].isEmpty()) {
				continue;
			}
			MapXContentBuilder xContentBuilder = new MapXContentBuilder(result);

			switch (infoFields[i]) {
			case KEY_OS:
				osService.stats().toXContent(new XContentBuilder(xContentBuilder, null), ToXContent.EMPTY_PARAMS);
				break;
			case KEY_JVM:
				jvmService.info().toXContent(new XContentBuilder(xContentBuilder, null), ToXContent.EMPTY_PARAMS);
				break;
			case KEY_PROCESS:
				processService.info().toXContent(new XContentBuilder(xContentBuilder, null), ToXContent.EMPTY_PARAMS);
				break;
			default:
				continue;
			}
		}

		return result;
	}

	public String getNodeId() {
		return nodeSettingsService.getNodeId();
	}

	public String getNodeName() {
		return nodeSettingsService.getNodeName();
	}
	
	private boolean checkIfDataNode() {
		String jdbcUrl = environment.getRequiredProperty("spring.datasource.url");
		if(jdbcUrl.contains("localhost")) {
			return true;
		}
		if(jdbcUrl.contains("127.0.0.1")) {
			return true;
		}
		if(jdbcUrl.contains(nodeSettingsService.getHostIp())) {
			return true;
		}
		return false;
	}
}
