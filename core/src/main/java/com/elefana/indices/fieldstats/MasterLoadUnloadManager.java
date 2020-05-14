/*******************************************************************************
 * Copyright 2020 Viridian Software Limited
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
package com.elefana.indices.fieldstats;

import com.elefana.indices.fieldstats.state.IllegalLoadingOperation;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.StateImpl;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldComponent;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.elefana.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MasterLoadUnloadManager implements LoadUnloadManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(MasterLoadUnloadManager.class);

	private final JdbcTemplate jdbcTemplate;
	private final State state;
	private final long indexTtl;

	private final ReadWriteLock loadUnloadLock = new ReentrantReadWriteLock();
	private final Set<String> missingIndices = new HashSet<String>();
	private final ThreadLocal<Set<String>> tmpMissingIndices = new ThreadLocal<Set<String>>() {
		@Override
		protected Set<String> initialValue() {
			return new HashSet<String>();
		}
	};

	private Map<String, Long> lastIndexUse = new ConcurrentHashMap<>();
	private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("elefana-fieldStats-loadUnloadManager"));

	public MasterLoadUnloadManager(JdbcTemplate jdbcTemplate, State state, long ttlMinutes) {
		this.jdbcTemplate = jdbcTemplate;
		this.state = state;
		this.indexTtl = ttlMinutes * 60 * 1000;

		missingIndices.addAll(jdbcTemplate.queryForList("SELECT _indexname FROM elefana_field_stats_index", String.class));
		scheduledExecutorService.scheduleAtFixedRate(this::unloadUnusedIndices, 0L, Math.max(ttlMinutes / 2, 1), TimeUnit.MINUTES);
	}

	private void unloadUnusedIndices() {
		long now = System.currentTimeMillis();
		lastIndexUse.forEach((index, timestamp) -> {
			if(now - timestamp > indexTtl) {
				loadUnloadLock.readLock().lock();
				try {
					if (!missingIndices.contains(index)) {
						loadUnloadLock.readLock().unlock();
						LOGGER.info("Index " + index + " wasn't used recently. Therefore it is being unloaded.");
						unloadIndex(index);
						loadUnloadLock.readLock().lock();
					}
				} finally {
					loadUnloadLock.readLock().unlock();
				}
			} else {
				loadUnloadLock.readLock().lock();
				try {
					if (missingIndices.contains(index)) {
						loadUnloadLock.readLock().unlock();
						LOGGER.info("Index " + index + " isn't outdated but not loaded. Therefore it is being loaded from the database.");
						loadIndex(index);
						loadUnloadLock.readLock().lock();
					}
				} finally {
					loadUnloadLock.readLock().unlock();
				}
			}
		});
	}

	public void shutdown() {
		scheduledExecutorService.shutdownNow();
		try {
			scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void someoneWroteToIndex(String indexName) {
		lastIndexUse.compute(indexName, (name, timestamp) -> System.currentTimeMillis());
	}

	public void ensureIndicesLoaded(String indexPattern) {
		loadUnloadLock.readLock().lock();
		try {
			final Set<String> missingIndices = tmpMissingIndices.get();
			missingIndices.clear();
			missingIndices.addAll(this.missingIndices);

			for(String missingIndex : missingIndices) {
				if (!StateImpl.matches(indexPattern, missingIndex)) {
					continue;
				}
				LOGGER.info("Request needs index " + missingIndex + ", so it is loaded from the database.");
				loadUnloadLock.readLock().unlock();
				loadIndex(missingIndex);
				lastIndexUse.compute(missingIndex, (name, timestamp) -> System.currentTimeMillis());
				loadUnloadLock.readLock().lock();
			}
		} finally {
			loadUnloadLock.readLock().unlock();
		}
	}

	private void loadIndex(String indexName) {
		loadUnloadLock.writeLock().lock();
		if(!missingIndices.contains(indexName)) {
			loadUnloadLock.writeLock().unlock();
			return;
		}
		try {
			IndexComponent indexComponent = jdbcTemplate.queryForObject(
					"SELECT * FROM elefana_field_stats_index WHERE _indexname = ?",
					(rs, rowNum) -> new IndexComponent(
							rs.getString("_indexname"),
							rs.getLong("_maxdocs")
					),
					indexName
			);
			if(indexComponent == null){
				LOGGER.warn("Tried to load an index not existing in the database");
			} else {
				jdbcTemplate.query(
						"SELECT fs.*, f._type FROM (SELECT * FROM elefana_field_stats_fieldstats fsi WHERE fsi._indexname = ?) fs INNER JOIN elefana_field_stats_field f ON fs._fieldname = f._fieldname",
						rs -> {
							String type = rs.getString("_type");
							String fieldName = rs.getString("_fieldname");
							try {
								Class tClass = Class.forName(type);
								indexComponent.fields.put(fieldName, new FieldComponent(
										rs.getString("_minvalue"),
										rs.getString("_maxvalue"),
										rs.getLong("_doccount"),
										rs.getLong("_sumdocfreq"),
										rs.getLong("_sumtotaltermfreq"),
										tClass
								));
							} catch (ClassNotFoundException e) {
								LOGGER.error("Class not found while loading fieldstats index from database. Not updating this field. DATABASE IS CORRUPT");
							}
						},
						indexName
				);
			}
			state.load(indexComponent);
			missingIndices.remove(indexName);
		} catch (ElefanaWrongFieldStatsTypeException e) {
			LOGGER.error(e.getMessage());
		} finally {
			loadUnloadLock.writeLock().unlock();
		}
	}

	private void unloadIndex(String indexName) {
		loadUnloadLock.writeLock().lock();
		if(missingIndices.contains(indexName)){
			loadUnloadLock.writeLock().unlock();
			// the database can only store one IndexComponent
			throw new IllegalLoadingOperation();
		}

		IndexComponent indexComponent = state.unload(indexName);

		missingIndices.add(indexName);

		long timestamp = System.currentTimeMillis();
		jdbcTemplate.update("INSERT INTO elefana_field_stats_index(_indexname, _maxdocs, _timestamp) VALUES (?, ?, ?) ON CONFLICT (_indexname) DO UPDATE SET " +
				"_maxdocs = excluded._maxdocs, _timestamp = excluded._timestamp", indexComponent.name, indexComponent.maxDocs, timestamp);

		indexComponent.fields.forEach((k, v) -> {
			jdbcTemplate.update(
					"INSERT INTO elefana_field_stats_fieldstats(_fieldname, _indexname, _doccount, _sumdocfreq, _sumtotaltermfreq, _minvalue, _maxvalue) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT ON CONSTRAINT _fieldstats_pk DO UPDATE SET _minvalue = excluded._minvalue, _maxvalue = excluded._maxvalue, _doccount = excluded._doccount, _sumdocfreq = excluded._sumdocfreq, _sumtotaltermfreq = excluded._sumtotaltermfreq",
					k, indexComponent.name, v.docCount, v.sumDocFreq, v.sumTotalTermFreq, v.minValue, v.maxValue
			);
			jdbcTemplate.update(
					"INSERT INTO elefana_field_stats_field (_fieldname, _type) VALUES (?, ?) ON CONFLICT (_fieldname) DO UPDATE SET _type = excluded._type",
					k, v.type.getName()
			);
		});
		loadUnloadLock.writeLock().unlock();
	}

	public void deleteIndex(String index) {
		loadUnloadLock.writeLock().lock();
		try {
			state.deleteIndex(index);
			missingIndices.remove(index);
			lastIndexUse.remove(index);
			jdbcTemplate.update("DELETE FROM elefana_field_stats_index WHERE _indexname = ?", index);
			jdbcTemplate.update("DELETE FROM elefana_field_stats_fieldstats WHERE _indexname = ?", index);
			jdbcTemplate.update("DELETE FROM elefana_field_stats_field field WHERE (SELECT count(_fieldname) FROM elefana_field_stats_fieldstats\n" +
					"ts WHERE _fieldname = field._fieldname ) = 0");
		} finally {
			loadUnloadLock.writeLock().unlock();
		}
	}

	public void unloadAll() {
		List<String> indices = state.compileIndexPattern("*");
		indices.forEach(index -> {
			loadUnloadLock.readLock().lock();
			if(missingIndices.contains(index)){
				loadUnloadLock.readLock().unlock();
				loadIndex(index);
				unloadIndex(index);
			} else {
				loadUnloadLock.readLock().unlock();
				unloadIndex(index);
			}
		});
	}
}
