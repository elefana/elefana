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
public class FieldImpl implements Field {
    private Map<String, FieldStats> fieldStats = new ConcurrentHashMap<>();
    private Class type;

    public FieldImpl(Class type) {
        this.type = type;
    }

    @Override
    public FieldStats getIndexFieldStats(String indexName) {
        return fieldStats.computeIfAbsent(indexName, key ->
                FieldComponent.getFieldStats(type)
        );
    }

    @Override
    public FieldStats getIndexFieldStats(Collection<String> indices) {
        FieldStats acc = FieldComponent.getFieldStats(type);
        for(String s : indices) {
            FieldStats fs = getIndexFieldStats(s);
            acc = acc.merge(fs);
        }
        return acc;
    }

    @Override
    public boolean hasIndexFieldStats(String name){
        return fieldStats.containsKey(name);
    }

    @Override
    public FieldStats getFieldStats() {
        return getIndexFieldStats(fieldStats.keySet());
    }

    @Override
    public void deleteIndexFieldStats(String indexName) {
        fieldStats.remove(indexName);
    }

    @Override
    public Class getFieldType() {
        return type;
    }

    @Override
    public void load(String indexName, FieldComponent fieldComponent) {
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
