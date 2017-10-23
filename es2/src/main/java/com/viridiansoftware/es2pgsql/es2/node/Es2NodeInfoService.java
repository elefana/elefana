/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.monitor.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.viridiansoftware.es2pgsql.es2.util.MapXContentBuilder;
import com.viridiansoftware.es2pgsql.node.NodeInfoService;

public class Es2NodeInfoService extends NodeInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeInfoService.class);
	
	private OsService osService;
	private ProcessService processService;
	private JvmService jvmService;
	
	@Override
	protected void initialiseEsServices() {
		final Settings settings = Settings.builder().put("node.name", nodeSettingsService.getNodeName()).build();
		
		osService = new OsService(settings, OsProbe.getInstance());
		processService = new ProcessService(settings, ProcessProbe.getInstance());
		jvmService = new JvmService(settings);
	}

	public Map<String, Object> getNodeInfo() throws IOException {
		return getNodeInfo(ALL_INFO);
	}

	@Override
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
}
