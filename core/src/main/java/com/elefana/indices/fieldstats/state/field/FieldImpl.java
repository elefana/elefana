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

package com.elefana.indices.fieldstats.state.field;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class FieldImpl<T> implements Field<T> {
    private Map<String, FieldStats<T>> fieldStats = new ConcurrentHashMap<>();
    private Class<T> type;

    public FieldImpl(Class<T> type) {
        this.type = type;
    }

    @Override
    public FieldStats<T> getIndexFieldStats(String indexName) {
        return fieldStats.computeIfAbsent(indexName, key ->
                Fields.getFieldStats(type)
        );
    }

    @Override
    public FieldStats<T> getIndexFieldStats(Collection<String> indices) {
        FieldStats<T> acc = Fields.getFieldStats(type);
        for(String s : indices) {
            FieldStats<T> fs = getIndexFieldStats(s);
            acc = acc.merge(fs);
        }
        return acc;
    }

    @Override
    public boolean hasIndexFieldStats(String name){
        return fieldStats.containsKey(name);
    }

    @Override
    public FieldStats<T> getFieldStats() {
        return getIndexFieldStats(fieldStats.keySet());
    }

    @Override
    public void deleteIndexFieldStats(String indexName) {
        fieldStats.remove(indexName);
    }

    @Override
    public Class<T> getFieldType() {
        return type;
    }

    @Override
    public void load(String indexName, FieldComponent<T> fieldComponent) {
        fieldStats.compute(indexName, (name, fieldStats) -> {
            if(fieldStats == null) {
                return fieldComponent.construct();
            } else {
                fieldStats.mergeAndModifySelf(fieldComponent.construct());
                return fieldStats;
            }
        });
    }

    public String getType() {
        return type.getName();
    }
}
