/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pg.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.digest.DigestUtils;
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

import com.viridiansoftware.es2pg.search.SearchService;
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

	private String nodeId;
	private String nodeName;
	private OsService osService;
	private ProcessService processService;
	private JvmService jvmService;
	// private final FsService fsService;
	
	@PostConstruct
	public void postConstruct() {
		LOGGER.info(System.getProperty("java.io.tmpdir"));
		
		nodeName = environment.getRequiredProperty("es2pgsql.node.name");
		nodeId = DigestUtils.sha1Hex(nodeName);
		
		final Settings settings = Settings.builder().put("node.name", nodeName).build();
		final ThreadPool threadPool = new ThreadPool(settings, new ExecutorBuilder[0]);

		osService = new OsService(settings);
		processService = new ProcessService(settings);
		jvmService = new JvmService(settings);
	}

	public Map<String, Object> getNodeInfo() throws IOException {
		return getNodeInfo(ALL_INFO);
	}

	public Map<String, Object> getNodeInfo(String... infoFields) throws IOException {
		Map<String, Object> result = new HashMap<String, Object>();

		for (int i = 0; i < infoFields.length; i++) {
			if(infoFields[i] == null) {
				continue;
			}
			if(infoFields[i].isEmpty()) {
				continue;
			}
			MapXContentBuilder xContentBuilder = new MapXContentBuilder();

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
			
			result.put(infoFields[i], xContentBuilder.getResult());
		}

		return result;
	}

	public String getNodeId() {
		return nodeId;
	}

	public String getNodeName() {
		return nodeName;
	}
}
