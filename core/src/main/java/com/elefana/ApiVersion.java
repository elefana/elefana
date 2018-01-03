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

public enum ApiVersion {
	V_2_4_3(2, 4, 3),
	V_5_5_2(5, 5, 2);
	
	private final int majorVersion;
	private final int minorVersion;
	private final int patchVersion;
	private final String versionString;

	private ApiVersion(int majorVersion, int minorVersion, int patchVersion) {
		this.majorVersion = majorVersion;
		this.minorVersion = minorVersion;
		this.patchVersion = patchVersion;
		this.versionString = majorVersion + "." + minorVersion + "." + patchVersion;
	}
	
	public boolean isNewerThan(ApiVersion apiVersion) {
		if(majorVersion > apiVersion.majorVersion) {
			return true;
		} else if(majorVersion < apiVersion.majorVersion) {
			return false;
		}
		
		if(minorVersion > apiVersion.minorVersion) {
			return true;
		} else if(minorVersion < apiVersion.minorVersion) {
			return false;
		}
		
		if(patchVersion > apiVersion.patchVersion) {
			return true;
		} else if(patchVersion < apiVersion.patchVersion) {
			return false;
		}
		return false;
	}
	
	public boolean isNewerThanOrEqualTo(ApiVersion apiVersion) {
		if(majorVersion > apiVersion.majorVersion) {
			return true;
		} else if(majorVersion < apiVersion.majorVersion) {
			return false;
		}
		
		if(minorVersion > apiVersion.minorVersion) {
			return true;
		} else if(minorVersion < apiVersion.minorVersion) {
			return false;
		}
		
		if(patchVersion > apiVersion.patchVersion) {
			return true;
		} else if(patchVersion < apiVersion.patchVersion) {
			return false;
		}
		return true;
	}

	public String getVersionString() {
		return versionString;
	}
}
