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

import com.elefana.document.BulkIndexTask;
import com.elefana.indices.fieldstats.state.field.*;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.elefana.indices.fieldstats.state.index.IndexImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jsoniter.output.JsonStream;
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
import java.util.stream.Collectors;

@ThreadSafe
public class StateImpl implements State{
    private static final Logger LOGGER = LoggerFactory.getLogger(StateImpl.class);

    private Map<String, Index> indexMap = new ConcurrentHashMap<>();
    private Map<String, Field> fieldMap = new ConcurrentHashMap<>();

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

    protected Index createIndex(String index) {
        return new IndexImpl();
    }

    protected Field createField(String fieldName, Class type) {
        return new FieldImpl(type);
    }

    @Override
    public void stopModificationsOfIndex(String index) {
        getIndex(index).getStopCountingLock().lock();
    }

    @Override
    public void resumeModificationsOfIndex(String index) {
        getIndex(index).getStopCountingLock().unlock();
    }

    @Override
    public void startIndexModification(String index) {
        getIndex(index).getCountingLock().lock();
    }

    @Override
    public void finishIndexModification(String index) {
        getIndex(index).getCountingLock().unlock();
    }

    @Override
    public void deleteIndex(String name) {
        stopModificationsOfIndex(name);
        deleteLockedIndex(name);
        resumeModificationsOfIndex(name);
    }

    private void deleteLockedIndex(String name) {
        getIndex(name).delete();
        fieldMap.forEach((fieldName, field) -> {
            field.deleteIndexFieldStats(name);
        });
    }

    @Override
    public void load(IndexComponent indexComponent) throws ElefanaWrongFieldStatsTypeException {
        startIndexModification(indexComponent.name);
        try {
            indexMap.compute(indexComponent.name, (name, index) -> {
                if (index == null) {
                    index = createIndex(name);
                }
                index.mergeAndModifySelf(indexComponent.construct());
                return index;
            });

            indexComponent.fields.forEach((fieldName, fieldComponent) -> {
                fieldMap.compute(fieldName, (name, field) -> {
                    if (field == null) {
                        Field newField = createField(name, fieldComponent.type);
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
            finishIndexModification(indexComponent.name);
        }
    }

    @Override
    public IndexComponent unload(String indexName) {
        stopModificationsOfIndex(indexName);

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

        resumeModificationsOfIndex(indexName);
        return indexComponent;
    }

    @Override
    public Index getIndex(String indexName) {
        return indexMap.computeIfAbsent(indexName, key -> createIndex(indexName));
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
        Field field = fieldMap.computeIfAbsent(fieldName, key -> createField(fieldName, tClass));

        if(!field.hasIndexFieldStats(index)) {
	        fieldNamesCache.invalidate(index);
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
        Field field = fieldMap.get(fieldName);
        if(field == null) {
            return null;
        } else {
            return field.getIndexFieldStats(indices);
        }
    }

    @Override
    @Nullable
    public FieldStats getFieldStats(String fieldName, String index) {
        Field field = fieldMap.get(fieldName);
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
        return indexMap
                .keySet()
                .stream()
                .filter(i -> matches(indexPattern, i))
                .collect(Collectors.toList());
    }

    public String serialize() {
        JsonStream.serialize(indexMap);
        return "";
    }

    public static boolean matches(String pattern, String index) {
        String[] singleIndices = pattern.split(",");
        for(String singleIndexPattern : singleIndices) {
            boolean matches = index.matches(singleIndexPattern.replaceAll("\\*", "\\.*"));
            if(matches) {
                return true;
            }
        }
        return false;
    }
}
