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

import com.jsoniter.annotation.JsonIgnore;
import com.jsoniter.annotation.JsonProperty;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class FieldImpl<T> implements Field<T> {
    private Map<String, FieldStats<T>> fieldStats = new ConcurrentHashMap<>();
    @JsonIgnore
    private Class<T> type;

    public FieldImpl(Class<T> type) {
        this.type = type;
    }

    public FieldImpl(Class<T> type, Map<String, FieldStats<T>> initialMap) {
        this(type);
        this.fieldStats = new ConcurrentHashMap<>(initialMap);
    }

    @Override
    public FieldStats<T> getIndexFieldStats(String indexName) {
        return fieldStats.computeIfAbsent(indexName, key ->
                FieldsImpl.getInstance().getFieldStats(type)
        );
    }

    @Override
    public FieldStats<T> getIndexFieldStats(Collection<String> indices) {
        FieldStats<T> acc = FieldsImpl.getInstance().getFieldStats(type);
        for(String s : indices) {
            FieldStats<T> fs = getIndexFieldStats(s);
            acc = acc.merge(fs);
        }
        return acc;
    }

    @JsonIgnore
    @Override
    public FieldStats<T> getFieldStats() {
        return getIndexFieldStats(fieldStats.keySet());
    }

    @Override
    public void deleteIndexFieldStats(String indexName) {
        fieldStats.remove(indexName);
    }

    @Override
    @JsonIgnore
    public Class<T> getFieldType() {
        return type;
    }

    @JsonProperty("type")
    public String getType() {
        return type.getName();
    }

    @JsonProperty("type")
    public void setType(String typeName) {
        try {
            this.type = (Class<T>)Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
