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

import com.elefana.indices.fieldstats.LoadUnloadManager;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJob;
import com.elefana.indices.fieldstats.job.CoreFieldStatsRemoveIndexJob;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.field.FieldComponent;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class StateTest {

    private static final String TEST_INDEX = "bank";

    private State testState;
    private LoadUnloadManager loadUnloadManager;

    private String testDocument;
    private String testDocumentNoBool;
    private String testDocumentNull;

    @Before
    public void before() {
        testState = new StateImpl(null);
        loadUnloadManager = mock(LoadUnloadManager.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                testState.deleteIndex((String)invocation.getArgument(0));
                return null;
            }
        }).when(loadUnloadManager).deleteIndex(anyString());
        testDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
        testDocumentNoBool = "{ \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
        testDocumentNull = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": null, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
    }

    @Test
    public void testSubmitDocument() {
        CoreFieldStatsJob job = CoreFieldStatsJob.allocate(testState, loadUnloadManager, TEST_INDEX);
        job.addDocument(testDocument);
        job.run();
    }

    @Test
    public void testSubmitMultipleDocument() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);
    }

    @Test
    public void testIndexReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        assertIndexMaxDocEquals(20, TEST_INDEX);
    }

    private void assertIndexMaxDocEquals(long expected, String index) {
        long indexMaxDocs = testState.getIndex(index).getMaxDocuments();
        Assert.assertEquals(expected, indexMaxDocs);
    }

    @Test
    public void testListFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats<Long> list = testState.getFieldStatsTypeChecked("list", Long.class, TEST_INDEX);
        Assert.assertEquals(20, list.getDocumentCount());
        Assert.assertEquals(8 * 20, list.getSumDocumentFrequency());
        Assert.assertEquals(2, list.getMinimumValue().longValue());
        Assert.assertEquals(6, list.getMaximumValue().longValue());
    }

    @Test
    public void testStringFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException{
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(20, string.getDocumentCount());
        Assert.assertEquals(40, string.getSumDocumentFrequency());
        Assert.assertEquals("hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    @Test
    public void testObjectFieldStatsReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats iban = testState.getFieldStats("obj.iban", TEST_INDEX);
        Assert.assertEquals(20, iban.getDocumentCount());
        Assert.assertEquals(40, iban.getSumDocumentFrequency());

        FieldStats bic = testState.getFieldStats("obj.bic", TEST_INDEX);
        Assert.assertEquals(20, bic.getDocumentCount());
        Assert.assertEquals(20, bic.getSumDocumentFrequency());
    }

    @Test
    public void testBooleanFieldStatsReturn() {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats bool = testState.getFieldStats("bool", TEST_INDEX);
        Assert.assertEquals(20, bool.getDocumentCount());
        Assert.assertEquals(20, bool.getSumDocumentFrequency());
    }

    @Test
    public void testSubmitFirstDocumentsAtSameTime() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(100, 1, testDocument, TEST_INDEX);

        FieldStats bool = testState.getFieldStats("bool", TEST_INDEX);
        Assert.assertEquals(100, bool.getDocumentCount());
        Assert.assertEquals(100, bool.getSumDocumentFrequency());

        assertIndexMaxDocEquals(100, TEST_INDEX);
        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(100, string.getDocumentCount());
        Assert.assertEquals(100 * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    @Test
    public void testSubmitMultipleDocumentsAtSameTime() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(100, 100, testDocument, TEST_INDEX);

        FieldStats bool = testState.getFieldStats("bool", TEST_INDEX);
        Assert.assertEquals(100 * 100, bool.getDocumentCount());
        Assert.assertEquals(100 * 100, bool.getSumDocumentFrequency());

        assertIndexMaxDocEquals(100 * 100, TEST_INDEX);
        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(100 * 100, string.getDocumentCount());
        Assert.assertEquals(100 * 100 * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());
    }

    private void submitDocumentNTimes(int n, String document, String index) {
        for(int i = 0; i < n; i++) {
            CoreFieldStatsJob job = CoreFieldStatsJob.allocate(testState, loadUnloadManager, index);
            job.addDocument(document);
            job.run();
        }
    }
    private void submitDocumentConcurrently(int numberOfThreads, int numberOfDocumentSubmissionsPerThread, String document, String index) {
        final CyclicBarrier barrier = new CyclicBarrier(numberOfThreads + 1);
        List<Thread> threadList = new ArrayList<>();
        for( int i = 0; i < numberOfThreads; i++) {
            threadList.add(new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                submitDocumentNTimes(numberOfDocumentSubmissionsPerThread, document, index);
            }));
        }
        threadList.forEach(Thread::start);
        try {
            barrier.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

        FieldStats bool = testState.getFieldStats("bool", ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, bool.getDocumentCount());
        Assert.assertEquals(docCount, bool.getSumDocumentFrequency());

        Assert.assertEquals(docCount, testState.getIndex(ImmutableList.of("first", "second")).getMaxDocuments());

        FieldStats string = testState.getFieldStats("string", ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, string.getDocumentCount());
        Assert.assertEquals(docCount * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("hello", (String)string.getMinimumValue());
        Assert.assertEquals("there", (String)string.getMaximumValue());
    }

    @Test
    public void testMergeFieldStatsNull() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentConcurrently(5, 10, testDocumentNull, "first");
        submitDocumentConcurrently(8, 5, testDocument, "second");

        long docCount = 5 * 10 + 8 * 5;
        long longCount = 8 * 5;

        FieldStats bool = testState.getFieldStats("bool", ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, bool.getDocumentCount());
        Assert.assertEquals(docCount, bool.getSumDocumentFrequency());

        Assert.assertEquals(docCount, testState.getIndex(ImmutableList.of("first", "second")).getMaxDocuments());

        Assert.assertEquals(0, testState.getFieldStats("long", "first").getDocumentCount());
        Assert.assertEquals(longCount, testState.getFieldStats("long", ImmutableList.of("first", "second")).getDocumentCount());

        Assert.assertEquals(0d, testState.getFieldStats("long", "first").getDensity(testState.getIndex("first")), 0.01d);
    }

    @Test
    public void testDeleteIndex() throws InterruptedException {
        submitDocumentConcurrently(10, 1, testDocument, TEST_INDEX);
        Thread a = new Thread(new CoreFieldStatsRemoveIndexJob(testState, loadUnloadManager, TEST_INDEX));
        a.start();
        a.join();
        submitDocumentConcurrently(1,1, testDocument, TEST_INDEX);

        Assert.assertEquals(1, testState.getIndex(TEST_INDEX).getMaxDocuments());
        Assert.assertEquals(1, testState.getFieldStats("bool", TEST_INDEX).getDocumentCount());
    }

    @Test
    public void testLoadIndex() throws ElefanaWrongFieldStatsTypeException {
        //submitDocumentNTimes(20, testDocument, TEST_INDEX);

        IndexComponent insert = new IndexComponent(TEST_INDEX, 10);
        insert.fields.put("string", new FieldComponent("hello", "there", 9, 9*2, -1, String.class));

        testState.load(insert);

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(9, string.getDocumentCount());
        Assert.assertEquals(9*2, string.getSumDocumentFrequency());
        Assert.assertEquals("hello", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());

        assertIndexMaxDocEquals(10, TEST_INDEX);
    }

    @Test
    public void testLoadIndexWithMerge() throws ElefanaWrongFieldStatsTypeException{
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        IndexComponent insert = new IndexComponent(TEST_INDEX, 10);
        insert.fields.put("string", new FieldComponent("aa", "s", 9, 9*2, -1, String.class));

        testState.load(insert);

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(9 + 20, string.getDocumentCount());
        Assert.assertEquals(9*2 + 20*2, string.getSumDocumentFrequency());
        Assert.assertEquals("aa", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());

        assertIndexMaxDocEquals(20 + 10, TEST_INDEX);
    }

    @Test
    public void testLoadIndexConcurrent() throws ElefanaWrongFieldStatsTypeException {
        IndexComponent insert = new IndexComponent(TEST_INDEX, 10);
        insert.fields.put("string", new FieldComponent("aa", "s", 9, 9*2, -1, String.class));

        List<Thread> threadList = new ArrayList<>();
        for( int i = 0; i < 50; i++) {
            threadList.add(new Thread(() -> submitDocumentNTimes(20, testDocument, TEST_INDEX)));
        }
        for(int i = 0; i < 10; i++) {
            threadList.add(new Thread(() -> {
                try {
                    testState.load(insert);
                } catch (ElefanaWrongFieldStatsTypeException e) {
                    e.printStackTrace();
                }
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

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(9*10 + 50*20, string.getDocumentCount());
        Assert.assertEquals(9*10*2 + 50*20*2, string.getSumDocumentFrequency());
        Assert.assertEquals("aa", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());

        assertIndexMaxDocEquals(10*10 + 50*20, TEST_INDEX);
    }

    @Test
    public void testUnloadIndex() {
        submitDocumentNTimes(10, testDocument, TEST_INDEX);

        IndexComponent index = testState.unload(TEST_INDEX);

        Assert.assertEquals(10, index.maxDocs);

        FieldComponent string = index.fields.get("string");

        Assert.assertEquals(10, string.docCount);
        Assert.assertEquals(10*2, string.sumDocFreq);
        Assert.assertEquals("hello", string.minValue);
        Assert.assertEquals("there", string.maxValue);
    }

    @Test(timeout=30000)
    public void testUnloadAndLoadIndexConcurrent() throws ElefanaWrongFieldStatsTypeException {
        final String index = TEST_INDEX + "2";

        List<Thread> threadList = new ArrayList<>();
        for( int i = 0; i < 50; i++) {
            threadList.add(new Thread(() -> submitDocumentNTimes(100, testDocument, index)));
        }
        for(int i = 0; i < 10; i++) {
            threadList.add(new Thread(() -> {
                try {
                    IndexComponent c = testState.unload(index);
                    testState.load(c);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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

        FieldStats result = testState.getFieldStatsTypeChecked("string", String.class, index);
        final int expectedDocCount = 50*100;
        final int expectedSumDocFrequency = 50*100*2;

        while(result.getDocumentCount() != expectedDocCount) {
            result = testState.getFieldStatsTypeChecked("string", String.class, index);

            try {
                Thread.sleep(100);
            } catch (Exception e) {}
        }
        Assert.assertEquals(expectedDocCount, result.getDocumentCount());
        Assert.assertEquals(expectedSumDocFrequency, result.getSumDocumentFrequency());
        Assert.assertEquals("hello", result.getMinimumValue());
        Assert.assertEquals("there", result.getMaximumValue());

        assertIndexMaxDocEquals(50*100, index);
    }
}
