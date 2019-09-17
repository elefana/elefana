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

import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.Field;
import com.elefana.indices.fieldstats.state.field.FieldImpl;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexImpl;

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
    private Map<String, Field<?>> fieldMap = new ConcurrentHashMap<>();

    @Override
    public void haltIndex(String index) {
        getIndex(index).getStopCountingLock().lock();
    }

    @Override
    public void resumeIndex(String index) {
        getIndex(index).getStopCountingLock().unlock();
    }

    @Override
    public void startIndexInsert(String index) {
        getIndex(index).getCountingLock().lock();
    }

    @Override
    public void finishIndexInsert(String index) {
        getIndex(index).getCountingLock().unlock();
    }

    @Override
    public void deleteIndex(String name) {
        haltIndex(name);
        try {
            indexMap.remove(name);
            fieldMap.forEach((fieldName, field) -> {
                field.deleteIndexFieldStats(name);
            });
        } finally {
            resumeIndex(name);
        }
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
    public <T> Field<T> getFieldTypeChecked(String fieldName, Class<T> tClass) throws ElefanaWrongFieldStatsTypeException {
        Field<?> field = fieldMap.computeIfAbsent(fieldName, key -> new FieldImpl<T>(tClass));

        if(tClass.equals(field.getFieldType()))
            return (Field<T>)field;

        throw new ElefanaWrongFieldStatsTypeException(fieldName, tClass);
    }

    @Override
    @Nullable
    public Field<?> getField(String fieldName) {
        return fieldMap.get(fieldName);
    }

    @Override
    public List<String> compileIndexPattern(String indexPattern) {
        return indexMap
                .keySet()
                .stream()
                .filter(i -> matches(indexPattern, i))
                .collect(Collectors.toList());
    }

    private boolean matches(String pattern, String index) {
        String[] singleIndices = pattern.split(",");
        for(String singleIndexPattern : singleIndices) {
            boolean matches = index.matches(singleIndexPattern.replaceAll("\\*", "\\.*"));
            if(matches)
                return true;
        }
        return false;
    }
}
