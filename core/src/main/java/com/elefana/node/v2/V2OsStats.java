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
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.VirtualMemory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class V2OsStats extends OsStats {

	private static final long MEASURE_NOT_AVAILABLE = -1L;

	private long[] recentCpuTicks;
	private HardwareAbstractionLayer hardware = new SystemInfo().getHardware();
	private CentralProcessor cpu = hardware.getProcessor();
	private GlobalMemory memory = hardware.getMemory();
	private VirtualMemory virtualMemory = memory.getVirtualMemory();

	@Override
	protected void generateCurrentStats(Map<String, Object> result) {
		result.clear();

		updateCpu(result);
		updateMemory(result);
		updateSwap(result);
		updateTimestamp(result);
	}

	private void updateCpu(Map<String, Object> osStatsObj) {
		updateCpuCurrentLoad(osStatsObj);
		updateCpuAverageLoad(osStatsObj);
	}

	private void updateCpuCurrentLoad(Map<String, Object> osStatsObj) {
		double cpuLoad = measureCpuCurrentLoad();

		long roundedCpuLoad = Math.round(cpuLoad * 100);
		osStatsObj.put("cpu_percent", rectifyMeasure(roundedCpuLoad));
	}

	private double measureCpuCurrentLoad() {
		initializeTicksWhenEmpty();
		double cpuLoad = cpu.getSystemCpuLoadBetweenTicks(recentCpuTicks);
		updateCpuTicks();
		return cpuLoad;
	}

	private void initializeTicksWhenEmpty() {
		if (recentCpuTicks == null) {
			updateCpuTicks();
		}
	}

	private void updateCpuTicks() {
		recentCpuTicks = cpu.getSystemCpuLoadTicks();
	}

	private void updateCpuAverageLoad(Map<String, Object> osStatsObj) {
		// First value in double array from getSystemLoadAverage is the average load for 1 minute
		double cpuLoadAverage = cpu.getSystemLoadAverage(1)[0];
		long roundedCpuLoadAverage = Math.round(cpuLoadAverage * 100);
		osStatsObj.put("load_average", rectifyMeasure(roundedCpuLoadAverage));
	}

	private void updateMemory(Map<String, Object> osStatsObj) {
		Map<String, Object> memMap = new HashMap<>();

		long totalMemory = memory.getTotal();
		memMap.put("total_in_bytes", totalMemory);

		long freeMemory = memory.getAvailable();
		memMap.put("free_in_bytes", freeMemory);

		long freePercent = Math.round(((double)freeMemory / totalMemory) * 100);
		memMap.put("free_percent", freePercent);

		long usedMemory = totalMemory - freeMemory;
		memMap.put("used_in_bytes", usedMemory);

		long usedPercent = Math.round(((double)usedMemory / totalMemory) * 100);
		memMap.put("used_percent", usedPercent);

		osStatsObj.put("mem", memMap);
	}

	private void updateSwap(Map<String, Object> osStatsObj) {
		Map<String, Object> swapMap = new HashMap<>();

		updateSwapTotal(swapMap);
		updateSwapUsed(swapMap);
		updateSwapFree(swapMap);

		osStatsObj.put("swap", swapMap);
	}

	private void updateSwapTotal(Map<String, Object> swapMap) {
		long totalSwap = virtualMemory.getSwapTotal();
		swapMap.put("total_in_bytes", totalSwap);
	}

	private void updateSwapUsed(Map<String, Object> swapMap) {
		long usedSwap = virtualMemory.getSwapUsed();
		swapMap.put("used_in_bytes", usedSwap);
	}

	private void updateSwapFree(Map<String, Object> swapMap) {
		long totalSwap = virtualMemory.getSwapTotal();
		long usedSwap = virtualMemory.getSwapUsed();
		long freeSwap = totalSwap - usedSwap;
		swapMap.put("free_in_bytes", freeSwap);
	}

	private void updateTimestamp(Map<String, Object> osStatsObj) {
		long timestamp = new Date().getTime();
		osStatsObj.put("timestamp", timestamp);
	}

	private long rectifyMeasure(long measure) {
		if (isValidOperatingSystemMeasure(measure)) {
			return measure;
		}
		return MEASURE_NOT_AVAILABLE;
	}

	private boolean isValidOperatingSystemMeasure(Long num) {
		if(num == null) {
			return false;
		}
		if(num < 0) {
			return false;
		}
		return true;
	}
}
