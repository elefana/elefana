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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@ThreadSafe
public class StateImpl implements State{
    private static final Logger LOGGER = LoggerFactory.getLogger(StateImpl.class);

    protected final Map<String, Index> indexMap = new HashMap<>();
    protected final Map<String, Field> fieldMap = new ConcurrentHashMap<>();

    protected final ReadWriteLock indexMapLock;
    protected Cache<String, Set<String>> fieldNamesCache;
    protected LoadingCache<String, List<String>> indexPatternCache;

    protected BiFunction<String, Class, Field> createFieldMethod = this::createFieldImplementation;

    public StateImpl(Environment environment) {
        this(environment, false);
    }

    public StateImpl(Environment environment, boolean fairLock) {
        indexMapLock = new ReentrantReadWriteLock(fairLock);
        if(environment != null) {
            fieldNamesCache = CacheBuilder.newBuilder().
                    maximumSize(environment.getProperty("elefana.service.field.cache.names.expire.size", Integer.class, 250)).
                    expireAfterAccess(environment.getProperty("elefana.service.field.cache.names.expire.time", Long.class, 500L), TimeUnit.SECONDS).
                    build();
        } else {
            fieldNamesCache = CacheBuilder.newBuilder().maximumSize(10).expireAfterAccess(1L, TimeUnit.SECONDS).build();
        }

        indexPatternCache = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.HOURS).maximumSize(128).build(new CacheLoader<String, List<String>>() {
            @Override
            public List<String> load(String key) throws Exception {
                return internalCompileIndexPattern(key);
            }
        });
    }

    protected Index createIndexImplementation(String index) {
        return new IndexImpl();
    }

    protected Field createFieldImplementation(String fieldName, Class type) {
        return new FieldImpl(type);
    }

    @Override
    public void deleteIndex(String name) {
        if(!isIndexLoaded(name)) {
            return;
        }
        final Index index = getIndex(name, false);
        if(index == null) {
            return;
        }
        deleteLockedIndex(name, index);
    }

    private void deleteLockedIndex(final String indexName, final Index index) {
        indexMapLock.writeLock().lock();

        try {
            fieldMap.forEach((fieldName, field) -> {
                field.deleteIndexFieldStats(indexName);
            });
            index.delete();
            indexMap.remove(indexName);
        } finally {
            indexMapLock.writeLock().unlock();
        }

        indexPatternCache.invalidateAll();
    }

    @Override
    public void load(IndexComponent indexComponent) throws ElefanaWrongFieldStatsTypeException {
        indexMapLock.writeLock().lock();
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
            indexMapLock.writeLock().unlock();
        }
    }

    @Override
    public IndexComponent unload(String indexName) {
        indexMapLock.writeLock().lock();
        if(!indexMap.containsKey(indexName)) {
            indexMapLock.writeLock().unlock();
            return null;
        }
        final IndexComponent indexComponent;

        try {
            final Index index = getIndex(indexName, false);
            indexComponent = new IndexComponent(indexName, index.getMaxDocuments());

            fieldMap.forEach((name, field) -> {
                if(!field.hasIndexFieldStats(indexName)) {
                    return;
                }
                FieldStats fieldStats = field.getIndexFieldStats(indexName);
                if(fieldStats == null) {
                    return;
                }
                FieldComponent fieldComponent = FieldComponent.getFieldComponent(fieldStats, field.getFieldType());
                indexComponent.fields.put(name, fieldComponent);
            });
            deleteLockedIndex(indexName, index);
        } finally {
            indexMapLock.writeLock().unlock();
        }
        return indexComponent;
    }

    @Override
    public IndexComponent snapshot(String indexName) {
        indexMapLock.readLock().lock();

        if(!indexMap.containsKey(indexName)) {
            indexMapLock.readLock().unlock();
            return null;
        }

        final IndexComponent indexComponent;
        try {
            Index index = getIndex(indexName);
            indexComponent = new IndexComponent(indexName, index.getMaxDocuments());

            fieldMap.forEach((name, field) -> {
                if(!field.hasIndexFieldStats(indexName)) {
                    return;
                }
                FieldStats fieldStats = field.getIndexFieldStats(indexName);
                if(fieldStats == null) {
                    return;
                }
                FieldComponent fieldComponent = FieldComponent.getFieldComponent(fieldStats, field.getFieldType());
                indexComponent.fields.put(name, fieldComponent);
            });
        } finally {
            indexMapLock.readLock().unlock();
        }
        return indexComponent;
    }

    @Override
    public Index getIndex(String indexName) {
        return getIndex(indexName, true);
    }

    private Index getIndex(String indexName, boolean createIfNonExistant) {
        indexMapLock.readLock().lock();
        while(!indexMap.containsKey(indexName)) {
            indexMapLock.readLock().unlock();

            if(!createIfNonExistant) {
                return null;
            }

            indexMapLock.writeLock().lock();

            try {
                if(!indexMap.containsKey(indexName)) {
                    indexMap.put(indexName, createIndexImplementation(indexName));
                }
                indexPatternCache.invalidateAll();
            } finally {
                indexMapLock.writeLock().unlock();
            }

            indexMapLock.readLock().lock();
        }
        final Index result = indexMap.get(indexName);
        indexMapLock.readLock().unlock();
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

    public void upgradeFieldToString(String fieldName) {
        indexMapLock.writeLock().lock();
        try {
            Field field = fieldMap.get(fieldName);

            final Map<String, FieldComponent> oldFieldStats = new HashMap<String, FieldComponent>();
            for(String oldIndex : indexMap.keySet()) {
                final FieldStats oldFieldStat = field.getIndexFieldStats(oldIndex);
                if(oldFieldStat == null) {
                    continue;
                }
                FieldComponent fieldComponent = FieldComponent.getFieldComponent(oldFieldStat, field.getFieldType());
                oldFieldStats.put(oldIndex, fieldComponent);
                field.deleteIndexFieldStats(oldIndex);
            }

            fieldMap.compute(fieldName, (key, value) -> {
                final Field newField = createFieldImplementation(key, String.class);
                for(String index : oldFieldStats.keySet()) {
                    newField.load(index, oldFieldStats.get(index));
                }
                return newField;
            });

        } finally {
            indexMapLock.writeLock().unlock();
        }
    }

    @Override
    @Nonnull
    public <T> FieldStats<T> getFieldStatsTypeChecked(String fieldName, Class<T> tClass, String index) throws ElefanaWrongFieldStatsTypeException {
        indexMapLock.readLock().lock();
        Field field = fieldMap.computeIfAbsent(fieldName, key -> createFieldMethod.apply(fieldName, tClass));
        indexMapLock.readLock().unlock();

        if(!field.hasIndexFieldStats(index)) {
	        fieldNamesCache.invalidate(index);
        }

        if(tClass.equals(field.getFieldType())) {
            return (FieldStats<T>) field.getIndexFieldStats(index);
        }
        if(tClass.equals(String.class) && field.getFieldType().equals(Long.class)) {
            upgradeFieldToString(fieldName);

            indexMapLock.readLock().lock();
            field = fieldMap.computeIfAbsent(fieldName, key -> createFieldMethod.apply(fieldName, tClass));
            indexMapLock.readLock().unlock();
            return (FieldStats<T>) field.getIndexFieldStats(index);
        }
        if(tClass.equals(String.class) && field.getFieldType().equals(Double.class)) {
            upgradeFieldToString(fieldName);

            indexMapLock.readLock().lock();
            field = fieldMap.computeIfAbsent(fieldName, key -> createFieldMethod.apply(fieldName, tClass));
            indexMapLock.readLock().unlock();
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
        final Field field = fieldMap.get(fieldName);
        if(field == null) {
            return null;
        } else {
            return field.getIndexFieldStats(indices);
        }
    }

    @Override
    @Nullable
    public FieldStats getFieldStats(String fieldName, String index) {
        final Field field = fieldMap.get(fieldName);
        if(field == null) {
            return null;
        } else {
            return field.getIndexFieldStats(index);
        }
    }

    @Override
    public void getFieldNames(Set<String> result, String index) {
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
        }
    }

    @Override
    public List<String> compileIndexPattern(String indexPattern) {
        try {
            return indexPatternCache.get(indexPattern);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return internalCompileIndexPattern(indexPattern);
    }

    private List<String> internalCompileIndexPattern(String indexPattern) {
        indexMapLock.readLock().lock();
        final List<String> result = indexMap
                .keySet()
                .stream()
                .filter(i -> matches(indexPattern, i))
                .collect(Collectors.toList());
        indexMapLock.readLock().unlock();
        return result;
    }

    @Override
    public <T> void ensureFieldExists(String fieldName, Class<T> fieldClass) {
        indexMapLock.readLock().lock();
        try {
            fieldMap.computeIfAbsent(fieldName, key -> createFieldMethod.apply(fieldName, fieldClass));
        } finally {
            indexMapLock.readLock().unlock();
        }
    }

    public boolean isIndexLoaded(String index) {
        indexMapLock.readLock().lock();
        final boolean result = indexMap.containsKey(index);
        indexMapLock.readLock().unlock();
        return result;
    }

    public boolean isIndexFieldsLoaded(String index) {
        for(Field field : fieldMap.values()) {
            if(field.hasIndexFieldStats(index)) {
                return true;
            }
        }
        return false;
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
