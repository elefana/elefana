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

import com.elefana.indices.fieldstats.job.CoreFieldStatsJob;
import com.elefana.indices.fieldstats.job.CoreFieldStatsRemoveIndexJob;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.google.common.collect.ImmutableList;
import com.jsoniter.JsonIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StateTest {

    private static final String TEST_INDEX = "bank";

    private State testState;

    private String testDocument;
    private String testDocumentNoBool;
    private String testDocumentNull;

    @Before
    public void before() {
        testState = new StateImpl();
        testDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
        testDocumentNoBool = "{ \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
        testDocumentNull = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": null, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
    }

    @Test
    public void testSubmitDocument() {
        CoreFieldStatsJob job = new CoreFieldStatsJob(JsonIterator.deserialize(testDocument), testState, TEST_INDEX);
        job.run();
    }

    @Test
    public void testSubmitMultipleDocument() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);
    }

    @Test
    public void testIndexReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        assertIndexMaxDocEquals(20);
    }

    private void assertIndexMaxDocEquals(long expected) {
        long indexMaxDocs = testState.getIndex(TEST_INDEX).getMaxDocuments();
        Assert.assertEquals(expected, indexMaxDocs);
    }

    @Test
    public void testListFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats<Long> list = testState.getFieldTypeChecked("list", Long.class).getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(20, list.getDocumentCount());
        Assert.assertEquals(8 * 20, list.getSumDocumentFrequency());
        Assert.assertEquals(2, list.getMinimumValue().longValue());
        Assert.assertEquals(6, list.getMaximumValue().longValue());
    }

    @Test
    public void testStringFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException{
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats string = testState.getFieldTypeChecked("string", String.class).getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(20, string.getDocumentCount());
        Assert.assertEquals(40, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    @Test
    public void testObjectFieldStatsReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats iban = testState.getField("obj.iban").getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(20, iban.getDocumentCount());
        Assert.assertEquals(40, iban.getSumDocumentFrequency());

        FieldStats bic = testState.getField("obj.bic").getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(20, bic.getDocumentCount());
        Assert.assertEquals(20, bic.getSumDocumentFrequency());
    }

    @Test
    public void testBooleanFieldStatsReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats bool = testState.getField("bool").getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(20, bool.getDocumentCount());
        Assert.assertEquals(20, bool.getSumDocumentFrequency());
    }

    @Test
    public void testSubmitFirstDocumentsAtSameTime() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(100, 1, testDocument, TEST_INDEX);

        FieldStats bool = testState.getField("bool").getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(100, bool.getDocumentCount());
        Assert.assertEquals(100, bool.getSumDocumentFrequency());

        assertIndexMaxDocEquals(100);
        FieldStats string = testState.getFieldTypeChecked("string", String.class).getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(100, string.getDocumentCount());
        Assert.assertEquals(100 * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    @Test
    public void testSubmitMultipleDocumentsAtSameTime() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(100, 100, testDocument, TEST_INDEX);

        FieldStats bool = testState.getField("bool").getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(100 * 100, bool.getDocumentCount());
        Assert.assertEquals(100 * 100, bool.getSumDocumentFrequency());

        assertIndexMaxDocEquals(100 * 100);
        FieldStats string = testState.getFieldTypeChecked("string", String.class).getIndexFieldStats(TEST_INDEX);
        Assert.assertEquals(100 * 100, string.getDocumentCount());
        Assert.assertEquals(100 * 100 * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    private void submitDocumentNTimes(int n, String document, String index) {
        for(int i = 0; i < n; i++) {
            CoreFieldStatsJob job = new CoreFieldStatsJob(JsonIterator.deserialize(document), testState, index);
            job.run();
        }
    }
    private void submitDocumentConcurrently(int numberOfThreads, int numberOfDocumentSubmissionsPerThread, String document, String index) {
        List<Thread> threadList = new ArrayList<>();
        for( int i = 0; i < numberOfThreads; i++) {
            threadList.add(new Thread(() -> {
                submitDocumentNTimes(numberOfDocumentSubmissionsPerThread, document, index);
            }));
        }
        threadList.forEach(Thread::start);
        threadList.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void testMergeFieldStats() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(5, 10, testDocument, "first");
        submitDocumentConcurrently(8, 5, testDocument, "second");

        long docCount = 5 * 10 + 8 * 5;

        FieldStats bool = testState.getField("bool").getIndexFieldStats(ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, bool.getDocumentCount());
        Assert.assertEquals(docCount, bool.getSumDocumentFrequency());

        Assert.assertEquals(docCount, testState.getIndex(ImmutableList.of("first", "second")).getMaxDocuments());

        FieldStats string = testState.getFieldTypeChecked("string", String.class).getIndexFieldStats(ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, string.getDocumentCount());
        Assert.assertEquals(docCount * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    @Test
    public void testMergeFieldStatsNull() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(5, 10, testDocumentNull, "first");
        submitDocumentConcurrently(8, 5, testDocument, "second");

        long docCount = 5 * 10 + 8 * 5;
        long longCount = 8 * 5;

        FieldStats bool = testState.getField("bool").getIndexFieldStats(ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, bool.getDocumentCount());
        Assert.assertEquals(docCount, bool.getSumDocumentFrequency());

        Assert.assertEquals(docCount, testState.getIndex(ImmutableList.of("first", "second")).getMaxDocuments());

        Assert.assertEquals(0, testState.getField("long").getIndexFieldStats("first").getDocumentCount());
        Assert.assertEquals(longCount, testState.getField("long").getIndexFieldStats(ImmutableList.of("first", "second")).getDocumentCount());

        Assert.assertEquals(0d, testState.getField("long").getIndexFieldStats("first").getDensity(testState.getIndex("first")), 0.01d);
    }

    @Test
    public void testDeleteIndex() throws InterruptedException {
        submitDocumentNTimes(10, testDocument, TEST_INDEX);
        Thread a = new Thread(new CoreFieldStatsRemoveIndexJob(testState, TEST_INDEX));
        a.start();
        a.join();
        submitDocumentNTimes(1, testDocument, TEST_INDEX);

        Assert.assertEquals(1, testState.getIndex(TEST_INDEX).getMaxDocuments());
        Assert.assertEquals(1, testState.getField("bool").getIndexFieldStats(TEST_INDEX).getDocumentCount());
    }
}
