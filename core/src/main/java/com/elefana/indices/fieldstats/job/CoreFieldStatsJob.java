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

import com.elefana.api.json.JsonUtils;
import com.elefana.document.BulkIndexOperation;
import com.elefana.indices.fieldstats.LoadUnloadManager;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.util.CumulativeAverage;
import com.elefana.util.PooledCharArray;
import com.elefana.util.PooledSimpleString;
import com.elefana.util.PooledStringBuilder;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@NotThreadSafe
public class CoreFieldStatsJob extends FieldStatsJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreFieldStatsJob.class);
    private static final Lock LOCK = new ReentrantLock();
    private static final List<CoreFieldStatsJob> POOL = new ArrayList<CoreFieldStatsJob>(32);
    private static final CumulativeAverage AVG_BATCH_SIZE = new CumulativeAverage(128);

    private static JsonFactory jsonFactory = new JsonFactory().setCodec(new ObjectMapper());

    private final List<PooledSimpleString> documents = new ArrayList<PooledSimpleString>(AVG_BATCH_SIZE.avg());
    private final Set<String> alreadyRegistered = new HashSet<>();

    private CoreFieldStatsJob(State state, LoadUnloadManager loadUnloadManager, String indexName) {
        super(state, loadUnloadManager, indexName);
    }

    public static CoreFieldStatsJob allocate(State state, LoadUnloadManager loadUnloadManager, String indexName) {
        LOCK.lock();
        final CoreFieldStatsJob result = POOL.isEmpty() ? null : POOL.remove(0);
        LOCK.unlock();
        if(result == null) {
            return new CoreFieldStatsJob(state, loadUnloadManager, indexName);
        }

        result.setIndexName(indexName);
        result.setLoadUnloadManager(loadUnloadManager);
        result.setState(state);
        return result;
    }

    public void release() {
        AVG_BATCH_SIZE.add(documents.size());

        documents.clear();
        alreadyRegistered.clear();

        LOCK.lock();
        POOL.add(this);
        LOCK.unlock();
    }

    public void addDocument(BulkIndexOperation bulkIndexOperation) {
        documents.add(PooledSimpleString.copyOf(bulkIndexOperation.getDocument(), bulkIndexOperation.getDocumentLength()));
    }

    public void addDocument(String document) {
        documents.add(PooledSimpleString.copyOf(document));
    }

    @Override
    public void run() {
        int docIndex = 0;

        for(docIndex = 0; docIndex < documents.size(); docIndex++) {
            try {
                alreadyRegistered.clear();
                final PooledSimpleString document = documents.get(docIndex);
                processAny(JsonUtils.extractJsonNode(document.getArray(), document.getLength()), "");
                document.release();

                loadUnloadManager.someoneWroteToIndex(indexName);
            } catch(Exception e) {
                LOGGER.error("Exception in Analyse Job", e);
            }
        }
        try {
            updateIndexMaxDocs(indexName, docIndex + 1);
        } catch(Exception e) {
            LOGGER.error("Exception in Analyse Job", e);
        }

        release();
    }

    private void processAny(TreeNode any, String prefix) {
        if (any.isObject()) {
            processObject((ObjectNode)any, prefix);
        } else if (any.isArray()) {
            processList((ArrayNode)any, prefix);
        } else if (any.isValueNode()) {
            ValueNode valueNode = (ValueNode)any;
            if (valueNode.isNumber()) {
                processNumber(valueNode, prefix);
            } else if (valueNode.isTextual()) {
                processString(valueNode, prefix);
            } else if (valueNode.isBoolean()) {
                processBoolean(valueNode, prefix);
            }
        } else {
            LOGGER.info("No type matched for document: " + any.toString());
        }
    }

    private void processObject(ObjectNode anyObject, String prefix) {
        Iterator<Map.Entry<String, JsonNode>> fields = anyObject.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldName = fields.next();
            final PooledStringBuilder stringBuilder = PooledStringBuilder.allocate();
            if(prefix.length() > 0) {
                stringBuilder.append(prefix);
                stringBuilder.append('.');
                stringBuilder.append(fieldName.getKey());
                processAny(fieldName.getValue(), stringBuilder.toStringAndRelease());
            } else {
                processAny(fieldName.getValue(), fieldName.getKey());
            }
        }
    }

    private void processList(ArrayNode anyList, String prefix) {
        Iterator<JsonNode> elements = anyList.elements();
        while(elements.hasNext()) {
            processAny(elements.next(), prefix);
        }
    }

    private void processNumber(ValueNode anyNumber, String prefix) {
        if (anyNumber.isIntegralNumber()) {
            long longNumber = anyNumber.asLong();
            updateFieldStats(prefix, Long.class, longNumber);
        } else if (anyNumber.isFloatingPointNumber()) {
            updateFieldStats(prefix, Double.class, anyNumber.asDouble());
        }
    }

    private void processBoolean(ValueNode anyBool, String prefix) {
        Boolean bool = anyBool.asBoolean();
        updateFieldStats(prefix, Boolean.class, bool);
    }

    private void processString(ValueNode anyString, String prefix) {
        String string = anyString.textValue();
        updateFieldStats(prefix, String.class, string);
    }

    private <T> void updateFieldStats(String fieldName, Class<T> tClass, T value) {
        try {
            FieldStats<T> fieldStats = state.getFieldStatsTypeChecked(fieldName, tClass, indexName);

            updateFieldStatsFoundOccurrence(fieldStats, value);
            if(alreadyRegistered.add(fieldName)) {
                updateFieldStatsIsInDocument(fieldStats);
            }
        } catch (ElefanaWrongFieldStatsTypeException e) {
            if(e.isTryParsing()) {
                updateFieldStats(fieldName, String.class, value.toString());
            } else {
                LOGGER.error("wrong field stats type", e);
            }
        }
    }

    private <T> void updateFieldStatsFoundOccurrence(FieldStats<T> fieldStats, T value){
        fieldStats.updateSingeOccurrence(value);
    }

    private <T> void updateFieldStatsIsInDocument(FieldStats<T> fieldStats) {
        fieldStats.updateFieldIsInDocument();
    }

    private void updateIndexMaxDocs(String indexName, int amount) {
        state.getIndex(indexName).incrementMaxDocuments(amount);
    }

    public int getTotalDocuments() {
        return documents.size();
    }
}

