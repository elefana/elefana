/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql;

import com.viridiansoftware.elefana.ElefanaApplication;

public abstract class ElefanaUAT {

	protected void startElefana(String testConfigPath) {
		ElefanaApplication.start(testConfigPath);
	}
	
	protected void stopElefana() {
		ElefanaApplication.stop();
	}
}
