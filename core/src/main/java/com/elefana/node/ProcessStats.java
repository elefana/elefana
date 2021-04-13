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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class ProcessStats implements Runnable {

	private final Map<String, Object> tempStats = new HashMap<String, Object>();
	private final Map<String, Object> currentStats = new ConcurrentHashMap<>();

	@Override
	public void run() {
		try {
			generateCurrentStats(tempStats);
			currentStats.putAll(tempStats);
		} catch (Exception e) {
		}
	}
	
	protected abstract void generateCurrentStats(Map<String, Object> result);

	public Map<String, Object> getCurrentStats() {
		return currentStats;
	}
}
