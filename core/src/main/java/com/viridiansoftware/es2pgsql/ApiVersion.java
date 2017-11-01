/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql;

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
