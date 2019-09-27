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

import com.elefana.indices.fieldstats.LoadUnloadManager;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class CoreFieldStatsAnalyseJob extends FieldStatsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreFieldStatsAnalyseJob.class);

    private static JsonFactory jsonFactory = new JsonFactory().setCodec(new ObjectMapper());

    protected String document;
    private Set<String> alreadyRegistered = new HashSet<>();

    protected CoreFieldStatsAnalyseJob(String document, State state, LoadUnloadManager loadUnloadManager, String indexName) {
        super(state, loadUnloadManager, indexName);
        this.document = document;
    }

    @Override
    public void run() {
        alreadyRegistered.clear();
        state.startIndexModification(indexName);
        try {
            processAny(jsonFactory.createParser(document).readValueAsTree(), new ArrayList<>());
            updateIndex(indexName);
            loadUnloadManager.someoneWroteToIndex(indexName);
        } catch(Exception e) {
            LOGGER.error("Exception in Analyse Job", e);
        } finally {
            state.finishIndexModification(indexName);
        }
    }

    private void processAny(TreeNode any, List<String> relName) {
        if (any.isObject()) {
            processObject((ObjectNode)any, relName);
        } else if (any.isArray()) {
            processList((ArrayNode)any, relName);
        } else if (any.isValueNode()) {
            ValueNode valueNode = (ValueNode)any;
            if (valueNode.isNumber()) {
                processNumber(valueNode, relName);
            } else if (valueNode.isTextual()) {
                processString(valueNode, relName);
            } else if (valueNode.isBoolean()) {
                processBoolean(valueNode, relName);
            }
        } else {
            LOGGER.info("No type matched for document: " + any.toString());
        }
    }

    private void processObject(ObjectNode anyObject, List<String> relName) {
        Iterator<Map.Entry<String, JsonNode>> fields = anyObject.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldName = fields.next();
            processAny(fieldName.getValue(), addToFieldName(relName, fieldName.getKey()));
        }
    }

    private List<String> addToFieldName(List<String> old, String n) {
        List<String> ret = new ArrayList<>(old);
        ret.add(n);
        return ret;
    }

    private void processList(ArrayNode anyList, List<String> relName) {
        Iterator<JsonNode> elements = anyList.elements();
        while(elements.hasNext()) {
            processAny(elements.next(), relName);
        }
    }

    private void processNumber(ValueNode anyNumber, List<String> relName) {
        if (anyNumber.isIntegralNumber()) {
            long longNumber = anyNumber.asLong();
            updateFieldStats(buildFieldName(relName), Long.class, longNumber);
        } else if (anyNumber.isFloatingPointNumber()) {
            updateFieldStats(buildFieldName(relName), Double.class, anyNumber.asDouble());
        }
    }

    private void processBoolean(ValueNode anyBool, List<String> relName) {
        Boolean bool = anyBool.asBoolean();
        updateFieldStats(buildFieldName(relName), Boolean.class, bool);
    }

    private void processString(ValueNode anyString, List<String> relName) {
        String string = anyString.asText();
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
            FieldStats<T> fieldStats = state.getFieldStatsTypeChecked(fieldName, tClass, indexName);

            updateFieldStatsFoundOccurrence(fieldStats, value);
            if(alreadyRegistered.add(fieldName))
                updateFieldStatsIsInDocument(fieldStats);
        } catch (ElefanaWrongFieldStatsTypeException e) {
            if(e.isTryParsing()) {
                updateFieldStats(fieldName, String.class, value.toString());
            } else {
                LOGGER.error("wrong field stats type", e);
            }
        }
    }

    protected abstract <T> void updateFieldStatsFoundOccurrence(FieldStats<T> fieldStats, T value);
    protected abstract <T> void updateFieldStatsIsInDocument(FieldStats<T> fieldStats);
    protected abstract void updateIndex(String indexName);
}