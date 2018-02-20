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
package com.elefana.api.cluster;

public class ClusterVersionInfo {
	private boolean buildSnapshot;
	private String luceneVersion;
	private String postgresqlVersion;
	private String number;
	private String buildHash;
	private String buildTimestamp;
	
	public boolean isBuildSnapshot() {
		return buildSnapshot;
	}
	
	public void setBuildSnapshot(boolean buildSnapshot) {
		this.buildSnapshot = buildSnapshot;
	}
	
	public String getLuceneVersion() {
		return luceneVersion;
	}
	
	public void setLuceneVersion(String luceneVersion) {
		this.luceneVersion = luceneVersion;
	}
	
	public String getPostgresqlVersion() {
		return postgresqlVersion;
	}
	
	public void setPostgresqlVersion(String postgresqlVersion) {
		this.postgresqlVersion = postgresqlVersion;
	}
	
	public String getNumber() {
		return number;
	}
	
	public void setNumber(String number) {
		this.number = number;
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
