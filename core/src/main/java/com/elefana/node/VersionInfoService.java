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

import com.elefana.ApiVersion;
import com.elefana.cluster.ClusterService;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.URL;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

@Component
public class VersionInfoService {
	private static final Logger LOGGER = LoggerFactory.getLogger(VersionInfoService.class);

	private static final String FALLBACK_TIMESTAMP = new DateTime().toString();

	@Autowired
	Environment environment;

	private ApiVersion apiVersion = ApiVersion.V_5_5_2;
	private String versionNumber;
	private String buildHash;
	private String buildTimestamp;

	@PostConstruct
	public void postConstruct() {
		final URL jarUrl = ClusterService.class.getProtectionDomain().getCodeSource().getLocation();
		final String jarUrlPath = jarUrl.toString();
		if (jarUrlPath.startsWith("file:/") && jarUrlPath.endsWith(".jar")) {
			try (JarInputStream jar = new JarInputStream(jarUrl.openStream())) {
				Manifest manifest = jar.getManifest();
				buildHash = manifest.getMainAttributes().getValue("Build-Hash");
				buildTimestamp = manifest.getMainAttributes().getValue("Build-Timestamp");
			} catch (Exception e) {
			}
		}
		switch (Integer.parseInt(environment.getRequiredProperty("elefana.esCompatVersion").trim())) {
		case 2:
			apiVersion = ApiVersion.V_2_4_3;
			break;
		case 5:
			apiVersion = ApiVersion.V_5_5_2;
			break;
		}

		versionNumber = apiVersion.getVersionString();
		if (buildHash == null) {
			buildHash = DigestUtils.sha1Hex(versionNumber);
		}
		if (buildTimestamp == null) {
			buildTimestamp = FALLBACK_TIMESTAMP;
		}
	}

	public ApiVersion getApiVersion() {
		return apiVersion;
	}

	public String getVersionNumber() {
		return versionNumber;
	}

	public void setVersionNumber(String versionNumber) {
		this.versionNumber = versionNumber;
	}

	public String getBuildHash() {
		return buildHash;
	}

	public void setBuildHash(String buildHash) {
		this.buildHash = buildHash;
	}

	public String getBuildTimestamp() {
		return buildTimestamp;
	}

	public void setBuildTimestamp(String buildTimestamp) {
		this.buildTimestamp = buildTimestamp;
	}
}
