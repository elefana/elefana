/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.cluster;

public class AckResponse {
	private final boolean acknowledged = true;

	public boolean isAcknowledged() {
		return acknowledged;
	}
}
