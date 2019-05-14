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
package com.elefana;

import java.io.File;
import java.util.Properties;

import org.mini2Dx.natives.OsInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextRefreshedEvent;

import com.jsoniter.JsonIterator;
import com.jsoniter.extra.PreciseFloatSupport;
import com.jsoniter.spi.DecodingMode;

@SpringBootApplication(exclude= { DataSourceAutoConfiguration.class })
@ComponentScan(basePackages = { "com.elefana" })
public class ElefanaApplication implements ApplicationListener<ContextRefreshedEvent> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ElefanaApplication.class);
	
	private static final String LINUX_CONFIGURATION_PATH = "/etc/elefana";
	private static final String MAC_CONFIGURATION_PATH = "/etc/elefana";
	private static final String WINDOWS_CONFIGURATION_PATH = "C:\\Program Files\\elefana";
	
	private static boolean APPLICATION_STARTED = false;
	private static ConfigurableApplicationContext APP_CONTEXT;
	
	static {
		PreciseFloatSupport.enable();
	}
	
	public static boolean isApplicationStarted() {
		return APPLICATION_STARTED;
	}

	public static void main(String[] args) {
		JsonIterator.setMode(DecodingMode.REFLECTION_MODE);

		SpringApplicationBuilder springApplicationBuilder = new SpringApplicationBuilder(ElefanaApplication.class);
		APP_CONTEXT = springApplicationBuilder.sources(ElefanaApplication.class).properties(getProperties(null)).bannerMode(Mode.OFF)
				.run(args);
	}

	public static void start(String testConfigDirectory) {
		JsonIterator.setMode(DecodingMode.REFLECTION_MODE);
		
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
		if(props.getProperty("spring.config.location") == null) {
			LOGGER.info("No config file found - using default internal configuration");
		} else {
			LOGGER.info("Using config " + props.getProperty("spring.config.location"));
		}
		return props;
	}
}
