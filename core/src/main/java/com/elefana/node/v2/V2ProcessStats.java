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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.elefana.node.ProcessStats;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class V2ProcessStats extends ProcessStats {

	private OperatingSystem operatingSystem = new SystemInfo().getOperatingSystem();
	private long maxOpenFileDescriptors = -1;

	private final Gauge<Long> gaugeCpuPercent, gaugeOpenDescriptors;

	public V2ProcessStats(MetricRegistry metricRegistry) {
		gaugeCpuPercent = metricRegistry.register(MetricRegistry.name("process", "cpu", "usage", "percent"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return Math.round(getOsProcess().calculateCpuPercent());
					}
				});
		gaugeOpenDescriptors = metricRegistry.register(MetricRegistry.name("process", "descriptors", "open"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return getOsProcess().getOpenFiles();
					}
				});
	}

	@Override
	protected void generateCurrentStats(Map<String, Object> result) {
		OSProcess osProcess = getOsProcess();

		updateFileHandles(result, osProcess);
		updateCpu(result, osProcess);
		updateMemory(result, osProcess);
		updateTimestamp(result);
	}

	private OSProcess getOsProcess() {
		int pid = operatingSystem.getProcessId();
		return operatingSystem.getProcess(pid);
	}

	private void updateFileHandles(Map<String, Object> result, OSProcess osProcess) {
		long openFileDescriptors = osProcess.getOpenFiles();
		maxOpenFileDescriptors = Math.max(maxOpenFileDescriptors, openFileDescriptors);

		result.put("open_file_descriptors", openFileDescriptors);
		result.put("max_file_descriptors", maxOpenFileDescriptors);
	}

	private void updateCpu(Map<String, Object> result, OSProcess osProcess) {
		result.compute("cpu", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}

			long cpuPercent = Math.round(osProcess.calculateCpuPercent());
			long cpuTime = osProcess.getKernelTime() + osProcess.getUserTime();

			map.put("percent", cpuPercent);
			map.put("total_in_millis", cpuTime);

			v = map;
			return v;
		});
	}

	private void updateMemory(Map<String, Object> result, OSProcess osProcess) {
		result.compute("mem", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}

			long totalVirtualMemory = osProcess.getVirtualSize();
			map.put("total_virtual_in_bytes", totalVirtualMemory);

			v = map;
			return v;
		});
	}

	private void updateTimestamp(Map<String, Object> result) {
		long timestamp = new Date().getTime();
		result.put("timestamp", timestamp);
	}

}
