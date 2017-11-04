/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.node.v2;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mini2Dx.natives.OsInformation;

import com.viridiansoftware.elefana.node.OsStats;

public class V2OsStats extends OsStats {

	@Override
	protected void generateCurrentStats(Map<String, Object> result) {
		// TODO Auto-generated method stub
		
	}
	
}
