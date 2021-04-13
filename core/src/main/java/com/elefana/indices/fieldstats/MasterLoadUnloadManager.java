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

import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.StateImpl;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldComponent;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.elefana.util.NamedThreadFactory;
import com.elefana.util.ThreadPriorities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class MasterLoadUnloadManager implements LoadUnloadManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(MasterLoadUnloadManager.class);

	private final JdbcTemplate jdbcTemplate;
	private final State state;
	private final long indexTtlMillis;

	private final ReadWriteLock loadUnloadLock = new ReentrantReadWriteLock();
	private final Set<String> missingIndices = new HashSet<String>();
	private final ThreadLocal<Set<String>> tmpMissingIndices = new ThreadLocal<Set<String>>() {
		@Override
		protected Set<String> initialValue() {
			return new HashSet<String>();
		}
	};

	private Map<String, Long> lastIndexUse = new ConcurrentHashMap<>();
	private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2,
			new NamedThreadFactory("elefana-fieldStats-loadUnloadManager", ThreadPriorities.LOAD_UNLOAD_MANAGER));

	public MasterLoadUnloadManager(JdbcTemplate jdbcTemplate, State state, boolean isMaster, long ttlMinutes, long snapshotMinutes) {
		this.jdbcTemplate = jdbcTemplate;
		this.state = state;
		if(ttlMinutes > 0) {
			this.indexTtlMillis = ttlMinutes * 60 * 1000;
		} else {
			this.indexTtlMillis = 60 * 1000;
		}

		missingIndices.addAll(jdbcTemplate.queryForList("SELECT _indexname FROM elefana_field_stats_index", String.class));

		scheduledExecutorService.scheduleAtFixedRate(this::unloadUnusedIndices, 0L, indexTtlMillis / 2, TimeUnit.MILLISECONDS);
		if(isMaster) {
			if(snapshotMinutes > 0) {
				scheduledExecutorService.scheduleAtFixedRate(this::snapshot, 0L, snapshotMinutes, TimeUnit.MINUTES);
			} else {
				scheduledExecutorService.scheduleAtFixedRate(this::snapshot, 0L, 30, TimeUnit.SECONDS);
			}
		}
	}

	private void snapshot() {
		try {
			List<String> indices = state.compileIndexPattern("*");
			indices.forEach(index -> {
				snapshotIndex(index);
			});
			LOGGER.info("Snapshotted stats for " + indices.size() + " indices");
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void unloadUnusedIndices() {
		try {
			long now = System.currentTimeMillis();
			lastIndexUse.forEach((index, timestamp) -> {
				if(now - timestamp < indexTtlMillis) {
					return;
				}

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
			});
		} catch(Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
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

	@Override
	public boolean isIndexLoaded(String indexName) {
		loadUnloadLock.readLock().lock();
		boolean result = !missingIndices.contains(indexName);
		loadUnloadLock.readLock().unlock();
		return result;
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
				if(loadIndex(missingIndex)) {
					lastIndexUse.compute(missingIndex, (name, timestamp) -> System.currentTimeMillis());
				}
				loadUnloadLock.readLock().lock();
			}
		} finally {
			loadUnloadLock.readLock().unlock();
		}
	}

	private boolean loadIndex(String indexName) {
		loadUnloadLock.writeLock().lock();
		if(!missingIndices.contains(indexName)) {
			loadUnloadLock.writeLock().unlock();
			return false;
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
		return true;
	}

	private void snapshotIndex(String indexName) {
		loadUnloadLock.readLock().lock();
		if(missingIndices.contains(indexName)) {
			loadUnloadLock.readLock().unlock();
			return;
		}
		try {
			IndexComponent indexComponent = state.snapshot(indexName);
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
		} finally {
			loadUnloadLock.readLock().unlock();
		}
	}

	private void unloadIndex(String indexName) {
		loadUnloadLock.writeLock().lock();
		if(missingIndices.contains(indexName)){
			loadUnloadLock.writeLock().unlock();
			return;
		}

		final IndexComponent indexComponent;
		try {
			indexComponent = state.unload(indexName);
			missingIndices.add(indexName);
		} finally {
			loadUnloadLock.writeLock().unlock();
		}

		if(indexComponent == null) {
			return;
		}

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
	}

	public void deleteIndex(String index) {
		loadUnloadLock.writeLock().lock();
		try {
			state.deleteIndex(index);
			missingIndices.remove(index);
			lastIndexUse.remove(index);
		} finally {
			loadUnloadLock.writeLock().unlock();
		}

		jdbcTemplate.update("DELETE FROM elefana_field_stats_index WHERE _indexname = ?", index);
		jdbcTemplate.update("DELETE FROM elefana_field_stats_fieldstats WHERE _indexname = ?", index);
//		jdbcTemplate.update("DELETE FROM elefana_field_stats_field field WHERE (SELECT count(_fieldname) FROM elefana_field_stats_fieldstats\n" +
//				"ts WHERE _fieldname = field._fieldname ) = 0");
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

	public boolean isMissingIndex(String index) {
		loadUnloadLock.readLock().lock();
		final boolean result = missingIndices.contains(index);
		loadUnloadLock.readLock().unlock();
		return result;
	}

	public List<String> compileIndexPattern(String indexPattern) {
		final List<String> resultA = state.compileIndexPattern(indexPattern);
		final Set<String> resultB = missingIndices
				.stream()
				.filter(i -> StateImpl.matches(indexPattern, i))
				.collect(Collectors.toSet());
		resultB.addAll(resultA);
		return new ArrayList<String>(resultB);
	}
}
