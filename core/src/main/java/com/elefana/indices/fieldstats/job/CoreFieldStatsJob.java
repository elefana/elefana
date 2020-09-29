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
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.document.BulkIndexOperation;
import com.elefana.indices.fieldstats.LoadUnloadManager;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.util.CumulativeAverage;
import com.elefana.util.NoAllocJsonReader;
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
public class CoreFieldStatsJob extends FieldStatsJob implements NoAllocJsonReader.JsonReaderListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreFieldStatsJob.class);
    private static final Lock LOCK = new ReentrantLock();
    private static final List<CoreFieldStatsJob> POOL = new ArrayList<CoreFieldStatsJob>(32);
    private static final CumulativeAverage AVG_BATCH_SIZE = new CumulativeAverage(128);

    public static boolean BASIC_MODE = false;

    private static JsonFactory jsonFactory = new JsonFactory().setCodec(new ObjectMapper());

    private final List<DocumentSourceProvider> documents = new ArrayList<DocumentSourceProvider>(AVG_BATCH_SIZE.avg());
    private final Set<String> alreadyRegistered = new HashSet<>();

    private final NoAllocJsonReader jsonReader = new NoAllocJsonReader();
    private final StringBuilder keyBuilder = new StringBuilder();

    private char [] keyBuffer = new char[128];
    private int keyBufferLength = 0;

    private int [] arrayContextStack = new int[32];
    private int arrayContextStackIndex = 0;

    private int [] underscorePositionStack = new int[32];
    private int underscoreStackIndex = 0;

    private int totalValuesAppended = 0;

    private boolean valueWritten = false;

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

        valueWritten = false;
        keyBufferLength = 0;
        arrayContextStackIndex = 0;
        underscoreStackIndex = 0;

        LOCK.lock();
        POOL.add(this);
        LOCK.unlock();
    }

    public void addDocument(BulkIndexOperation bulkIndexOperation) {
        documents.add(bulkIndexOperation);
    }

    public void addDocument(String document) {
        documents.add(SingleDocumentSourceProvider.allocate(document));
    }

    public void addDocument(PooledStringBuilder document) {
        documents.add(SingleDocumentSourceProvider.allocate(document));
    }

    @Override
    public void run() {
        int docIndex = 0;

        for(docIndex = 0; docIndex < documents.size(); docIndex++) {
            try {
                alreadyRegistered.clear();
                final DocumentSourceProvider document = documents.get(docIndex);
                PooledStringBuilder str = PooledStringBuilder.allocate();
                str.append(document.getDocument(), 0, document.getDocumentLength());
                jsonReader.read(str, this);
                str.release();
                document.dispose();

                loadUnloadManager.someoneWroteToIndex(indexName);
            } catch(Exception e) {
                LOGGER.error("Exception in Analyse Job", e);
            }
        }
        try {
            updateIndexMaxDocs(indexName, docIndex);
        } catch(Exception e) {
            LOGGER.error("Exception in Analyse Job", e);
        }

        release();
    }

    private void processNumber(Number anyNumber, String prefix) {
        if (anyNumber instanceof Double || anyNumber instanceof Float ) {
            updateFieldStats(prefix, Double.class, anyNumber.doubleValue());
        } else {
            long longNumber = anyNumber.longValue();
            updateFieldStats(prefix, Long.class, longNumber);
        }
    }

    private void processBoolean(boolean bool, String prefix) {
        updateFieldStats(prefix, Boolean.class, bool);
    }

    private void processString(String value, String prefix) {
        if(value == null) {
            return;
        }
        updateFieldStats(prefix, String.class, value);
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

    @Override
    public boolean onReadBegin() {
        return true;
    }

    @Override
    public boolean onReadEnd() {
        return true;
    }

    @Override
    public boolean onObjectBegin() {
        pushDot(false);
        return true;
    }

    @Override
    public boolean onObjectEnd() {
        popDot();
        resetKey();
        return true;
    }

    @Override
    public boolean onArrayBegin() {
        pushDot(true);
        return true;
    }

    @Override
    public boolean onArrayEnd() {
        popDot();

        if(!valueWritten) {
            valueWritten = true;
        }
        resetKey();
        return true;
    }

    @Override
    public boolean onKey(char[] value, int from, int length) {
        appendToKeyBuffer(value, from, length);
        valueWritten = false;
        return true;
    }

    @Override
    public boolean onValue(char[] value, int from, int length) {
        keyBuilder.append(keyBuffer, 0, keyBufferLength);

        int startOffset = 0;
        int trimLength = length;
        for(int i = 0; i < length; i++) {
            if(value[from + i] == ' ') {
                continue;
            }
            startOffset = i;
            break;
        }
        for(int i = length; i > 0; i--) {
            if(value[from + i - 1] == ' ') {
                continue;
            }
            trimLength = i;
            break;
        }
        trimLength -= startOffset;

        statField(keyBuilder.toString(), value, from + startOffset, trimLength);
        keyBuilder.setLength(0);

        totalValuesAppended++;
        resetKey();
        valueWritten = true;
        return true;
    }

    private void statField(String key, char [] value, int from, int length) {
        key.intern();

        if(value[from] == '\'' || value[from] == '\"') {
            if(BASIC_MODE) {
                processString("", key);
            } else {
                processString(new String(value, from + 1, length - 2), key);
            }
        } else if(isBoolValue(value, from, length)) {
            processBoolean(value[from] == 't' || value[from] == 'T', key);
        } else if(isNullValue(value, from, length)) {
            processString(null, key);
        } else if(isFloatingPointValue(value, from, length)) {
            if(BASIC_MODE) {
                processNumber(0f, key);
            } else {
                processNumber(Float.valueOf(new String(value, from, length)), key);
            }
        } else if(isDoubleValue(value, from, length)) {
            if(BASIC_MODE) {
                processNumber(0.0, key);
            } else {
                processNumber(Double.valueOf(new String(value, from, length)), key);
            }
        } else {
            if(BASIC_MODE) {
                processNumber(0L, key);
            } else {
                processNumber(Long.valueOf(new String(value, from, length)), key);
            }
        }
    }

    private boolean isFloatingPointValue(char [] value, int from, int length) {
        if(value[from + length - 1] != 'f') {
            return false;
        }
        return true;
    }

    private boolean isDoubleValue(char [] value, int from, int length) {
        for(int i = from; i < from + length; i++) {
            if(value[i] == '.') {
                return true;
            }
        }
        return false;
    }

    private boolean isNullValue(char [] value, int from, int length) {
        if(length != 4) {
            return false;
        }
        switch(value[from]) {
        case 'n':
        case 'N':
            if(value[from + 1] != 'u' && value[from + 1] != 'U') {
                return false;
            }
            if(value[from + 2] != 'l' && value[from + 2] != 'L') {
                return false;
            }
            if(value[from + 3] != 'l' && value[from + 3] != 'L') {
                return false;
            }
            return true;
        }
        return false;
    }

    private boolean isBoolValue(char [] value, int from, int length) {
        if(length != 4 && length != 5) {
            return false;
        }
        switch(value[from]) {
        case 'f':
        case 'F':
            if(value[from + 1] != 'a' && value[from + 1] != 'A') {
                return false;
            }
            if(value[from + 2] != 'l' && value[from + 2] != 'L') {
                return false;
            }
            if(value[from + 3] != 's' && value[from + 3] != 'S') {
                return false;
            }
            if(value[from + 4] != 'e' && value[from + 4] != 'E') {
                return false;
            }
            return true;
        case 't':
        case 'T':
            if(value[from + 1] != 'r' && value[from + 1] != 'R') {
                return false;
            }
            if(value[from + 2] != 'u' && value[from + 2] != 'U') {
                return false;
            }
            if(value[from + 3] != 'e' && value[from + 3] != 'E') {
                return false;
            }
            return true;
        }
        return false;
    }

    private void pushDot(boolean arrayContext) {
        if(underscoreStackIndex > 0 && !arrayContext) {
            appendToKeyBuffer('.');
        }
        ensureUnderscoreStackCapacity(underscoreStackIndex);
        ensureArrayContextStackCapacity(arrayContextStackIndex);

        underscorePositionStack[underscoreStackIndex] = keyBufferLength;
        underscoreStackIndex++;

        arrayContextStack[arrayContextStackIndex] = arrayContext ? 0 : -1;
        arrayContextStackIndex++;
    }

    private void popDot() {
        if(underscoreStackIndex > 0) {
            underscoreStackIndex--;
            keyBufferLength = underscorePositionStack[underscoreStackIndex];

            arrayContextStackIndex--;
        } else {
            keyBufferLength = 0;
        }
    }

    private void resetKey() {
        if(underscoreStackIndex > 0) {
            keyBufferLength = underscorePositionStack[underscoreStackIndex - 1];
        } else {
            keyBufferLength = 0;
        }
    }

    private boolean isArrayContext() {
        if(arrayContextStackIndex > 0) {
            return arrayContextStack[arrayContextStackIndex - 1] > -1;
        }
        return false;
    }

    private int getArrayIndex() {
        if(arrayContextStackIndex > 0) {
            if(arrayContextStack[arrayContextStackIndex - 1] > -1) {
                return arrayContextStack[arrayContextStackIndex - 1];
            }
        }
        return 0;
    }

    private void incrementArrayIndex() {
        if(arrayContextStackIndex > 0) {
            if(arrayContextStack[arrayContextStackIndex - 1] > -1) {
                arrayContextStack[arrayContextStackIndex - 1]++;
            }
        }
    }

    private void appendToKeyBuffer(char c) {
        ensureKeyBufferCapacity(keyBufferLength + 1);
        keyBuffer[keyBufferLength] = c;
        keyBufferLength++;
    }

    private void appendToKeyBuffer(int value) {
        if(value < 10 && value >= 0) {
            ensureKeyBufferCapacity(keyBufferLength + 1);
            keyBuffer[keyBufferLength] = (char)(value + '0');
            keyBufferLength++;
        } else {
            final String valueAsString = String.valueOf(value);
            ensureKeyBufferCapacity(keyBufferLength + valueAsString.length());
            for(int i = 0; i < valueAsString.length(); i++) {
                keyBuffer[keyBufferLength] = valueAsString.charAt(i);
                keyBufferLength++;
            }
        }
    }

    private void appendToKeyBuffer(char [] chars, int from, int length) {
        ensureKeyBufferCapacity(keyBufferLength + length);
        System.arraycopy(chars, from, keyBuffer, keyBufferLength, length);
        keyBufferLength += length;
    }

    private void ensureKeyBufferCapacity(int capacity) {
        if(keyBuffer.length > capacity) {
            return;
        }
        final char [] newBuffer = new char[capacity * 2];
        System.arraycopy(keyBuffer, 0, newBuffer, 0, keyBuffer.length);
        keyBuffer = newBuffer;
    }

    private void ensureUnderscoreStackCapacity(int capacity) {
        if(underscorePositionStack.length > capacity) {
            return;
        }
        final int [] newBuffer = new int[capacity * 2];
        System.arraycopy(underscorePositionStack, 0, newBuffer, 0, underscorePositionStack.length);
        underscorePositionStack = newBuffer;
    }

    private void ensureArrayContextStackCapacity(int capacity) {
        if(arrayContextStack.length > capacity) {
            return;
        }
        final int [] newBuffer = new int[capacity * 2];
        System.arraycopy(arrayContextStack, 0, newBuffer, 0, arrayContextStack.length);
        arrayContextStack = newBuffer;
    }
}

