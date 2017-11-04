/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana;

import java.io.File;
import java.util.Properties;

import org.mini2Dx.natives.OsInformation;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@SpringBootApplication
@ComponentScan(basePackages = { "com.viridiansoftware.elefana" })
public class ElefanaApplication implements ApplicationListener<ContextRefreshedEvent> {
	private static final String LINUX_CONFIGURATION_PATH = "/etc/elefana";
	private static final String MAC_CONFIGURATION_PATH = "/etc/elefana";
	private static final String WINDOWS_CONFIGURATION_PATH = "C:\\Program Files\\elefana";
	
	private static boolean APPLICATION_STARTED = false;
	private static ConfigurableApplicationContext APP_CONTEXT;
	
	public static boolean isApplicationStarted() {
		return APPLICATION_STARTED;
	}

	public static void main(String[] args) {
		SpringApplicationBuilder springApplicationBuilder = new SpringApplicationBuilder(ElefanaApplication.class);
		APP_CONTEXT = springApplicationBuilder.sources(ElefanaApplication.class).properties(getProperties(null)).bannerMode(Mode.OFF)
				.run(args);
	}

	public static void start(String testConfigDirectory) {
		SpringApplicationBuilder springApplicationBuilder = new SpringApplicationBuilder(ElefanaApplication.class);
		APP_CONTEXT = springApplicationBuilder.sources(ElefanaApplication.class).properties(getProperties(testConfigDirectory))
				.bannerMode(Mode.OFF).run();
	}
	
	public static void stop() {
		APP_CONTEXT.close();
	}
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		APPLICATION_STARTED = true;
	}

	@Bean
	public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter(mapper);
		return converter;
	}

	static Properties getProperties(String configDirectory) {
		Properties props = new Properties();
		if (configDirectory != null) {
			props.put("spring.config.location", "file:" + configDirectory);
		} else {
			switch (OsInformation.getOs()) {
			case WINDOWS:
				if (new File(WINDOWS_CONFIGURATION_PATH).exists()) {
					props.put("spring.config.location", "file:" + WINDOWS_CONFIGURATION_PATH);
				}
				break;
			case MAC:
				if (new File(MAC_CONFIGURATION_PATH).exists()) {
					props.put("spring.config.location", "file:" + MAC_CONFIGURATION_PATH);
				}
				break;
			case UNIX:
			default:
				if (new File(LINUX_CONFIGURATION_PATH).exists()) {
					props.put("spring.config.location", "file:" + LINUX_CONFIGURATION_PATH);
				}
				break;
			}
		}
		return props;
	}
}
