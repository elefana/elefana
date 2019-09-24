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
import com.jsoniter.output.JsonStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@ThreadSafe
public class StateImpl implements State{
    private Map<String, Index> indexMap = new ConcurrentHashMap<>();
    private Map<String, Field> fieldMap = new ConcurrentHashMap<>();

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
                    return indexComponent.construct();
                } else {
                    index.mergeAndModifySelf(indexComponent.construct());
                    return index;
                }
            });

            indexComponent.fields.forEach((fieldName, fieldComponent) -> {
                fieldMap.compute(fieldName, (name, field) -> {
                    if (field == null) {
                        Field newField = fieldComponent.constructField();
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
        return indexMap.computeIfAbsent(indexName, key -> new IndexImpl());
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
        Field field = fieldMap.computeIfAbsent(fieldName, key -> new FieldImpl(tClass));

        if(tClass.equals(field.getFieldType()))
            return (FieldStats<T>)field.getIndexFieldStats(index);

        throw new ElefanaWrongFieldStatsTypeException(fieldName, tClass);
    }

    @Override
    @Nonnull
    public <T> FieldStats<T> getFieldStatsTypeChecked(String fieldName, Class<T> tClass, Collection<String> indices) throws ElefanaWrongFieldStatsTypeException {
        Field field = fieldMap.computeIfAbsent(fieldName, key -> new FieldImpl(tClass));

        if(tClass.equals(field.getFieldType()))
            return (FieldStats<T>)field.getIndexFieldStats(indices);

        throw new ElefanaWrongFieldStatsTypeException(fieldName, tClass);
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
            if(matches)
                return true;
        }
        return false;
    }
}
