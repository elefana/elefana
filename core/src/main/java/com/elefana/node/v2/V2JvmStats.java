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
import com.elefana.node.JvmStats;

import java.lang.management.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class V2JvmStats extends JvmStats {

	private RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
	private ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
	private List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
	private ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
	private MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	private List<BufferPoolMXBean> bufferPoolMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
	private List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();

	private final Gauge<Long> gaugeHeapUsed, gaugeHeapMax, gaugeNonHeapUsed;
	private final Gauge<Integer> gaugeThreadsCount;

	public V2JvmStats(MetricRegistry metricRegistry) {
		gaugeHeapUsed = metricRegistry.register(MetricRegistry.name("jvm", "heap", "usedBytes"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return memoryMXBean.getHeapMemoryUsage().getUsed();
					}
				});
		gaugeHeapMax = metricRegistry.register(MetricRegistry.name("jvm", "heap", "maxBytes"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return memoryMXBean.getHeapMemoryUsage().getMax();
					}
				});
		gaugeNonHeapUsed = metricRegistry.register(MetricRegistry.name("jvm", "nonheap", "usedBytes"),
				new Gauge<Long>() {
					@Override
					public Long getValue() {
						return memoryMXBean.getNonHeapMemoryUsage().getUsed();
					}
				});
		gaugeThreadsCount = metricRegistry.register(MetricRegistry.name("jvm", "threads"),
				new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						return threadMXBean.getThreadCount();
					}
				});
	}

	@Override
	protected void generateCurrentStats(Map<String, Object> result) {
		updateUptime(result);
		updateThreads(result);
		updateGarbageCollection(result);
		updateClasses(result);
		updateMemory(result);
		updateBufferPools(result);
		updateTimestamp(result);
	}

	private void updateUptime(Map<String, Object> result) {
		long uptime = runtimeMXBean.getUptime();
		result.put("uptime_in_millis", uptime);
	}

	private void updateThreads(Map<String, Object> result) {
		long threadsCount = threadMXBean.getThreadCount();
		long peakThreadCount = threadMXBean.getPeakThreadCount();

		result.compute("threads", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}
			generateMap(map,
					getKVP("count", threadsCount),
					getKVP("peak_count", peakThreadCount)
			);
			v = map;
			return v;
		});
	}

	private void updateGarbageCollection(Map<String, Object> result) {
		result.compute("gc", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}
			generateMap(map,
					getKVPIterate("collectors", gcMXBeans, GarbageCollectorMXBean::getName, garbageCollectorMXBean -> generateMap(
							getKVP("collection_count", garbageCollectorMXBean.getCollectionCount()),
							getKVP("collection_time_in_millis", garbageCollectorMXBean.getCollectionTime())
					))
			);
			v = map;
			return v;
		});
	}

	private void updateClasses(Map<String, Object> result) {
		long totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
		long currentLoadedClassCount = classLoadingMXBean.getLoadedClassCount();
		long unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

		result.compute("classes", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}

			map.put("current_loaded_count", currentLoadedClassCount);
			map.put("total_loaded_count", totalLoadedClassCount);
			map.put("total_unloaded_count", unloadedClassCount);

			v = map;
			return v;
		});
	}

	private void updateMemory(Map<String, Object> result) {
		long heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
		long heapMax = memoryMXBean.getHeapMemoryUsage().getMax();
		long heapCommited = memoryMXBean.getHeapMemoryUsage().getCommitted();
		long heapPercent = (int)Math.round(((double)heapUsed / heapMax) * 100);

		long nonHeapUsed = memoryMXBean.getNonHeapMemoryUsage().getUsed();
		long nonHeapCommited = memoryMXBean.getNonHeapMemoryUsage().getCommitted();

		result.compute("mem", (k, v) -> {
			final Map<String, Object> map;
			if(v == null) {
				map = new HashMap<String, Object>();
			} else {
				map = (HashMap<String, Object>) v;
			}
			map.put("heap_used_in_bytes", heapUsed);
			map.put("heap_used_percent", heapPercent);
			map.put("heap_committed_in_bytes", heapCommited);
			map.put("heap_max_in_bytes", heapMax);
			map.put("non_heap_used_in_bytes", nonHeapUsed);
			map.put("non_heap_committed_in_bytes", nonHeapCommited);

			generateMap(map,
					getKVPIterate("pools", memoryPoolMXBeans, MemoryPoolMXBean::getName, memoryPoolMXBean -> generateMap(
							getKVP("used_in_bytes", memoryPoolMXBean.getUsage().getUsed()),
							getKVP("max_in_bytes", memoryPoolMXBean.getUsage().getMax()),
							getKVP("peak_used_in_bytes", memoryPoolMXBean.getPeakUsage().getUsed()),
							getKVP("peak_max_in_bytes", memoryPoolMXBean.getPeakUsage().getMax())
					))
			);

			v = map;
			return v;
		});
	}

	private void updateBufferPools(Map<String, Object> result) {
		KeyValuePair bufferPools = getKVPIterate("buffer_pools", bufferPoolMXBeans, BufferPoolMXBean::getName, bufferPoolMXBean -> generateMap(
				getKVP("count", bufferPoolMXBean.getCount()),
				getKVP("used_in_bytes", bufferPoolMXBean.getMemoryUsed()),
				getKVP("total_capacity_in_bytes", bufferPoolMXBean.getTotalCapacity())
		));
		result.put(bufferPools.key, bufferPools.value);
	}

	private void updateTimestamp(Map<String, Object> result) {
		long timestamp = new Date().getTime();
		result.put("timestamp", timestamp);
	}

	private Map<String, Object> generateMap(KeyValuePair... values) {
		Map<String, Object> childMap = new HashMap<>();
		generateMap(childMap, values);
		return childMap;
	}

	private void generateMap(Map<String, Object> result, KeyValuePair... values) {
		Stream.of(values).forEach(v -> result.put(v.key, v.value));
	}

	private Map<String, Object> generateMap(Stream<KeyValuePair> values) {
		Map<String, Object> childMap = new HashMap<>();
		generateMap(childMap, values);
		return childMap;
	}

	private void generateMap(Map<String, Object> result, Stream<KeyValuePair> values) {
		values.forEach(v -> result.put(v.key, v.value));
	}

	private KeyValuePair getKVP(String key, Object value) {
		if(key != null && key.contains("'")) {
			key = key.replace("'", "_");
		}
		return new KeyValuePair(key, value);
	}

	private <B> KeyValuePair getKVPIterate(String key, List<B> list, Function<B, String> generateKey, Function<B, Object> generateValue) {
		if(key != null && key.contains("'")) {
			key = key.replace("'", "_");
		}
		return getKVP(key, generateMap(list
				.stream()
				.map(b -> getKVP(generateKey.apply(b), generateValue.apply(b)))
		));
	}

	private class KeyValuePair {
		private String key;
		private Object value;

		private KeyValuePair(String key, Object value) {
			this.key = key;
			this.value = value;
		}
	}
}
