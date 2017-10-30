/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql;

public enum ApiVersion {
	V_2_4_3("2.4.3"),
	V_5_5_2("5.5.2");
	
	private final String versionString;

	private ApiVersion(String versionString) {
		this.versionString = versionString;
	}

	public String getVersionString() {
		return versionString;
	}
}
