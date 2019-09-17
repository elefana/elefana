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

package com.elefana.indices.fieldstats.job;

import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.Field;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.google.common.math.DoubleMath;
import com.jsoniter.any.Any;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@NotThreadSafe
public class CoreFieldStatsJob extends FieldStatsJob {

    protected Any document;
    private Set<String> alreadyRegistered = new HashSet<>();

    public CoreFieldStatsJob(Any document, State state, String indexName) {
        super(state, indexName);
        this.document = document;
    }

    @Override
    public void run() {
        alreadyRegistered.clear();
        state.startIndexInsert(indexName);
        try {
            processAny(document, new ArrayList<>());
            updateIndex(indexName);
        } finally {
            state.finishIndexInsert(indexName);
        }
    }

    private void processAny(Any any, List<String> relName) {
        switch (any.valueType()) {
            case OBJECT:
                processObject(any, relName);
                break;
            case ARRAY:
                processList(any, relName);
                break;
            case STRING:
                processString(any, relName);
                break;
            case NUMBER:
                processNumber(any, relName);
                break;
            case BOOLEAN:
                processBoolean(any, relName);
                break;
            case NULL:
            case INVALID:
            default:
                break;
        }
    }

    private void processObject(Any anyObject, List<String> relName) {
        anyObject.asMap().forEach((key, value) -> {
            processAny(value, addToFieldName(relName, key));
        });
    }

    private List<String> addToFieldName(List<String> old, String n) {
        List<String> ret = new ArrayList<>(old);
        ret.add(n);
        return ret;
    }

    private void processList(Any anyList, List<String> relName) {
        for(Any any : anyList.asList()) {
            processAny(any, relName);
        }
    }

    private void processNumber(Any anyNumber, List<String> relName) {
        double doubleNumber = anyNumber.toDouble();
        if (DoubleMath.isMathematicalInteger(doubleNumber)) {
            long longNumber = (long) doubleNumber;
            updateFieldStats(buildFieldName(relName), Long.class, longNumber);
        } else {
            updateFieldStats(buildFieldName(relName), Double.class, doubleNumber);
        }
    }

    private void processBoolean(Any anyBool, List<String> relName) {
        Boolean bool = anyBool.toBoolean();
        updateFieldStats(buildFieldName(relName), Boolean.class, bool);
    }

    private void processString(Any anyString, List<String> relName) {
        String string = anyString.toString();
        updateFieldStats(buildFieldName(relName), String.class, string);
    }

    private String buildFieldName(List<String> name) {
        StringBuilder builder = new StringBuilder();
        int i;
        for(i = 0; i < name.size() - 1; i++)
            builder.append(name.get(i)).append('.');
        builder.append(name.get(i));
        return builder.toString();
    }

    private <T> void updateFieldStats(String fieldName, Class<T> tClass, T value) {
        try {
            Field<T> field = state.getFieldTypeChecked(fieldName, tClass);
            FieldStats<T> fieldStats = field.getIndexFieldStats(indexName);

            updateFieldStatsFoundOccurrence(fieldStats, value);
            if(alreadyRegistered.add(fieldName))
                updateFieldStatsIsInDocument(fieldStats);
        } catch (ElefanaWrongFieldStatsTypeException e) {
            e.printStackTrace();
        }
    }

    private <T> void updateFieldStatsFoundOccurrence(FieldStats<T> fieldStats, T value){
        fieldStats.updateSingeOccurrence(value);
    }

    private <T> void updateFieldStatsIsInDocument(FieldStats<T> fieldStats) {
        fieldStats.updateFieldIsInDocument();
    }

    private void updateIndex(String indexName) {
        state.getIndex(indexName).incrementMaxDocuments();
    }
}

