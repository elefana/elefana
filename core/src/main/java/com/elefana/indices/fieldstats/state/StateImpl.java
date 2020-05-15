/*******************************************************************************
 * Copyright 2019 Viridian Software Limited
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

package com.elefana.indices.fieldstats.state;

import com.elefana.indices.fieldstats.state.field.*;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.elefana.indices.fieldstats.state.index.IndexImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@ThreadSafe
public class StateImpl implements State{
    private static final Logger LOGGER = LoggerFactory.getLogger(StateImpl.class);

    private final Map<String, Index> indexMap = new HashMap<>();
    private final Map<String, Field> fieldMap = new ConcurrentHashMap<>();

    private final ReadWriteLock indexLock = new ReentrantReadWriteLock();
    private Cache<String, Set<String>> fieldNamesCache;

    public StateImpl(Environment environment) {
        if(environment != null) {
            fieldNamesCache = CacheBuilder.newBuilder().
                    maximumSize(environment.getProperty("elefana.service.field.cache.names.expire.size", Integer.class, 250)).
		            expireAfterAccess(environment.getProperty("elefana.service.field.cache.names.expire.time", Long.class, 500L), TimeUnit.SECONDS).
                    build();
        } else {
	        fieldNamesCache = CacheBuilder.newBuilder().maximumSize(10).expireAfterAccess(1L, TimeUnit.SECONDS).build();
        }
    }

    protected Index createIndexImplementation(String index) {
        return new IndexImpl();
    }

    protected Field createFieldImplementation(String fieldName, Class type) {
        return new FieldImpl(type);
    }

    @Override
    public void deleteIndex(String name) {
        indexLock.writeLock().lock();
        deleteLockedIndex(name);
        indexLock.writeLock().unlock();
    }

    private void deleteLockedIndex(String indexName) {
        fieldMap.forEach((fieldName, field) -> {
            field.deleteIndexFieldStats(indexName);
        });
        getIndex(indexName).delete();
        indexMap.remove(indexName);
    }

    @Override
    public void load(IndexComponent indexComponent) throws ElefanaWrongFieldStatsTypeException {
        indexLock.writeLock().lock();
        try {
            indexMap.compute(indexComponent.name, (name, index) -> {
                if (index == null) {
                    index = createIndexImplementation(name);
                }
                index.mergeAndModifySelf(indexComponent.construct());
                return index;
            });

            indexComponent.fields.forEach((fieldName, fieldComponent) -> {
                fieldMap.compute(fieldName, (name, field) -> {
                    if (field == null) {
                        Field newField = createFieldImplementation(name, fieldComponent.type);
                        newField.load(indexComponent.name, fieldComponent);
                        return newField;
                    } else {
                        if (field.getFieldType() != fieldComponent.type) {
                            return field;
                        } else {
                            field.load(indexComponent.name, fieldComponent);
                            return field;
                        }
                    }
                });
            });
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public IndexComponent unload(String indexName) {
        indexLock.writeLock().lock();

        Index index = getIndex(indexName);
        IndexComponent indexComponent = new IndexComponent(indexName, index.getMaxDocuments());

        fieldMap.forEach((name, field) -> {
            if(field.hasIndexFieldStats(indexName)) {
                FieldStats fieldStats = field.getIndexFieldStats(indexName);
                FieldComponent fieldComponent = FieldComponent.getFieldComponent(fieldStats, field.getFieldType());
                indexComponent.fields.put(name, fieldComponent);
            }
        });

        deleteLockedIndex(indexName);

        indexLock.writeLock().unlock();
        return indexComponent;
    }

    @Override
    public IndexComponent snapshot(String indexName) {
        indexLock.writeLock().lock();

        Index index = getIndex(indexName);
        IndexComponent indexComponent = new IndexComponent(indexName, index.getMaxDocuments());

        fieldMap.forEach((name, field) -> {
            if(field.hasIndexFieldStats(indexName)) {
                FieldStats fieldStats = field.getIndexFieldStats(indexName);
                FieldComponent fieldComponent = FieldComponent.getFieldComponent(fieldStats, field.getFieldType());
                indexComponent.fields.put(name, fieldComponent);
            }
        });
        indexLock.writeLock().unlock();
        return indexComponent;
    }

    @Override
    public Index getIndex(String indexName) {
        indexLock.readLock().lock();
        while(!indexMap.containsKey(indexName)) {
            indexLock.readLock().unlock();
            indexLock.writeLock().lock();

            if(!indexMap.containsKey(indexName)) {
                indexMap.put(indexName, createIndexImplementation(indexName));
            }

            indexLock.writeLock().unlock();
            indexLock.readLock().lock();
        }
        final Index result = indexMap.get(indexName);
        indexLock.readLock().unlock();
        return result;
    }

    @Override
    public Index getIndex(Collection<String> indices) {
        Index acc = new IndexImpl();

        for(String s : indices) {
            Index i = getIndex(s);
            acc = acc.merge(i);
        }
        return acc;
    }

    @Override
    @Nonnull
    public <T> FieldStats<T> getFieldStatsTypeChecked(String fieldName, Class<T> tClass, String index) throws ElefanaWrongFieldStatsTypeException {
        indexLock.readLock().lock();
        Field field = fieldMap.computeIfAbsent(fieldName, key -> createFieldImplementation(fieldName, tClass));
        indexLock.readLock().unlock();

        if(!field.hasIndexFieldStats(index)) {
            indexLock.readLock().lock();
	        fieldNamesCache.invalidate(index);
            indexLock.readLock().unlock();
        }

        if(tClass.equals(field.getFieldType())) {
            return (FieldStats<T>) field.getIndexFieldStats(index);
        }
        if((tClass.equals(Long.class) || tClass.equals(Double.class)) && field.getFieldType().equals(String.class)) {
            throw new ElefanaWrongFieldStatsTypeException(fieldName, tClass, field.getFieldType(), true);
        }
        throw new ElefanaWrongFieldStatsTypeException(fieldName, tClass, field.getFieldType());
    }

    @Override
    @Nullable
    public FieldStats getFieldStats(String fieldName, Collection<String> indices) {
        indexLock.readLock().lock();
        final Field field = fieldMap.get(fieldName);
        indexLock.readLock().unlock();
        if(field == null) {
            return null;
        } else {
            return field.getIndexFieldStats(indices);
        }
    }

    @Override
    @Nullable
    public FieldStats getFieldStats(String fieldName, String index) {
        indexLock.readLock().lock();
        final Field field = fieldMap.get(fieldName);
        indexLock.readLock().unlock();
        if(field == null) {
            return null;
        } else {
            return field.getIndexFieldStats(index);
        }
    }

    @Override
    public void getFieldNames(Set<String> result, String index) {
        indexLock.readLock().lock();
        try {
            result.addAll(fieldNamesCache.get(index, new Callable<Set<String>>() {
                @Override
                public Set<String> call() throws Exception {
                    final Set<String> result = new HashSet<String>();
                    for(String fieldName : fieldMap.keySet()) {
                        final Field field = fieldMap.get(fieldName);
                        if(field == null) {
                            continue;
                        } else if(field.hasIndexFieldStats(index)) {
                            result.add(fieldName);
                        }
                    }
                    return result;
                }
            }));
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    @Override
    public List<String> compileIndexPattern(String indexPattern) {
        indexLock.readLock().lock();
        final List<String> result = indexMap
                .keySet()
                .stream()
                .filter(i -> matches(indexPattern, i))
                .collect(Collectors.toList());
        indexLock.readLock().unlock();
        return result;
    }

    @Override
    public <T> void ensureFieldExists(String fieldName, Class<T> fieldClass) {
        indexLock.readLock().lock();
        fieldMap.computeIfAbsent(fieldName, key -> createFieldImplementation(fieldName, fieldClass));
        indexLock.readLock().unlock();
    }

    public boolean isIndexLoaded(String index) {
        indexLock.readLock().lock();
        final boolean result = indexMap.containsKey(index);
        indexLock.readLock().unlock();
        return result;
    }

    public static boolean matches(String pattern, String index) {
        if(!pattern.contains(",")) {
            pattern = pattern.replace(".", "\\.");
            pattern = pattern.replace("-", "\\-");
            pattern = pattern.replace("*", "(.*)");
            pattern = "^" + pattern + "$";
            return index.matches(pattern);
        } else {
            String[] singleIndices = pattern.split(",");
            for(String singleIndexPattern : singleIndices) {
                singleIndexPattern = singleIndexPattern.replace(".", "\\.");
                singleIndexPattern = singleIndexPattern.replace("-", "\\-");
                singleIndexPattern = singleIndexPattern.replace("*", "(.*)");
                singleIndexPattern = "^" + singleIndexPattern + "$";
                boolean matches = index.matches(singleIndexPattern);
                if(matches) {
                    return true;
                }
            }
        }
        return false;
    }
}
