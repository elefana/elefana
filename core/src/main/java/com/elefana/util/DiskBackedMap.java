/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.util;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DiskBackedMap<K, V> implements Map<K, V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(DiskBackedMap.class);
	private static final String MAPS_DIRECTORY = "maps";

	private final String mapId;
	private final Class<K> keyClass;
	private final Class<V> valueClass;

	private final AtomicBoolean disposed = new AtomicBoolean(false);
	private ChronicleMap<K, V> chronicleMap;

	public DiskBackedMap(String mapId, Class<K> keyClass, Class<V> valueClass, File dataDirectory,
						int expectedEntries, K averageKey, V averageValue) {
		this(mapId, keyClass, valueClass, dataDirectory, expectedEntries, averageKey, averageValue, false);
	}

	public DiskBackedMap(String mapId, Class<K> keyClass, Class<V> valueClass, File dataDirectory,
	                     int expectedEntries, K averageKey, V averageValue, boolean cleanImmediately) {
		this.mapId = mapId;
		this.keyClass = keyClass;
		this.valueClass = valueClass;

		if(!dataDirectory.exists()) {
			dataDirectory.mkdirs();
		}
		final File mapsDirectory = new File(dataDirectory, MAPS_DIRECTORY);
		if(!mapsDirectory.exists()) {
			mapsDirectory.mkdirs();
		}

		try {
			ChronicleMapBuilder mapBuilder = ChronicleMap.of(keyClass, valueClass)
					.name(mapId).entries(expectedEntries);
			if(!keyClass.equals(Integer.class) && !keyClass.equals(Long.class)) {
				mapBuilder = mapBuilder.averageKey(averageKey);
			}
			if(!valueClass.equals(Integer.class) && !valueClass.equals(Long.class)) {
				mapBuilder = mapBuilder.averageValue(averageValue);
			}
			chronicleMap = mapBuilder.createOrRecoverPersistedTo(new File(mapsDirectory, mapId));

			if(cleanImmediately) {
				chronicleMap.clear();
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void dispose() {
		if(disposed.getAndSet(true)) {
			return;
		}
		try {
			chronicleMap.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	public int size() {
		if(disposed.get()) {
			return 0;
		}
		return chronicleMap.size();
	}

	@Override
	public boolean isEmpty() {
		if(disposed.get()) {
			return true;
		}
		return chronicleMap.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		if(disposed.get()) {
			return false;
		}
		return chronicleMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		if(disposed.get()) {
			return false;
		}
		return chronicleMap.containsValue(value);
	}

	@Override
	public V get(Object key) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.get(key);
	}

	@Nullable
	@Override
	public V put(K key, V value) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.put(key, value);
	}

	@Override
	public V remove(Object key) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.remove(key);
	}

	@Override
	public void putAll(@NotNull Map<? extends K, ? extends V> m) {
		if(disposed.get()) {
			return;
		}
		chronicleMap.putAll(m);
	}

	@Override
	public void clear() {
		if(disposed.get()) {
			return;
		}
		chronicleMap.clear();
	}

	@NotNull
	@Override
	public Set<K> keySet() {
		if(disposed.get()) {
			return new HashSet<>();
		}
		return chronicleMap.keySet();
	}

	@NotNull
	@Override
	public Collection<V> values() {
		if(disposed.get()) {
			return new LinkedList<>();
		}
		return chronicleMap.values();
	}

	@NotNull
	@Override
	public Set<Entry<K, V>> entrySet() {
		if(disposed.get()) {
			return new HashSet<>();
		}
		return chronicleMap.entrySet();
	}

	@Override
	public V compute(K key, @NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.compute(key, remappingFunction);
	}

	@Override
	public V computeIfAbsent(K key, @NotNull Function<? super K, ? extends V> mappingFunction) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.computeIfAbsent(key, mappingFunction);
	}

	@Override
	public V computeIfPresent(K key, @NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.computeIfPresent(key, remappingFunction);
	}

	@Nullable
	@Override
	public V putIfAbsent(K key, V value) {
		if(disposed.get()) {
			return null;
		}
		return chronicleMap.putIfAbsent(key, value);
	}
}
