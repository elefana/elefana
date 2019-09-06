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
package com.elefana.node.v2;

import com.elefana.node.OsStats;
import oshi.SystemInfo;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class V2OsStats extends OsStats {

	@Override
	protected void generateCurrentStats(Map<String, Object> result) {
		result.clear();
		SystemInfo systemInfo = new SystemInfo();

		double cpuLoad = systemInfo.getHardware().getProcessor().getSystemCpuLoad();
		int roundedCpuLoad = (int)Math.round(cpuLoad * 100);
		result.put("percent", roundedCpuLoad);

		// First value in double array from getSystemLoadAverage is the average load for 1 minute
		double cpuLoadAverage = systemInfo.getHardware().getProcessor().getSystemLoadAverage(1)[0];
		int roundedCpuLoadAverage = (int)Math.round(cpuLoad * 100);
		result.put("load_average", roundedCpuLoadAverage);

		Map<String, Object> mem = new HashMap<>();

		long totalMemory = systemInfo.getHardware().getMemory().getTotal();
		mem.put("total_in_bytes", totalMemory);

		long freeMemory = systemInfo.getHardware().getMemory().getAvailable();
		mem.put("free_in_bytes", freeMemory);

		long freePercent = (int)Math.round((freeMemory / totalMemory) * 100);
		mem.put("free_percent", freePercent);

		long usedMemory = totalMemory - freeMemory;
		mem.put("used_in_bytes", usedMemory);

		long usedPercent = (int)Math.round((usedMemory / totalMemory) * 100);
		mem.put("used_percent", usedPercent);

		result.put("mem", mem);

		Map<String, Object> swap = new HashMap<>();

		long totalSwap = systemInfo.getHardware().getMemory().getSwapTotal();
		swap.put("total_in_bytes", usedPercent);

		long usedSwap = systemInfo.getHardware().getMemory().getSwapUsed();
		swap.put("used_in_bytes", usedPercent);

		long freeSwap = totalSwap - usedSwap;
		swap.put("free_in_bytes", usedPercent);

		result.put("swap", swap);

		long timestamp = new Date().getTime();
		result.put("timestamp", timestamp);
	}
}
