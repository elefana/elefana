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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class StateImplTest {

    private static final String TEST_INDEX = "bank";

    private StateImpl testState;
    private LoadUnloadManager loadUnloadManager;

    private String testDocument;
    private String testDocumentNoBool;
    private String testDocumentNull;

    @Before
    public void before() {
        testState = new StateImpl(null, true);
        loadUnloadManager = mock(LoadUnloadManager.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                testState.deleteIndex((String)invocation.getArgument(0));
                return null;
            }
        }).when(loadUnloadManager).deleteIndex(anyString());
        testDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2], \"listObjects\": [{\"field\": 77},{\"field\": 78}] } ";
        testDocumentNoBool = "{ \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
        testDocumentNull = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": null, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
    }

    @Test
    public void testMatches() {
        Assert.assertTrue(StateImpl.matches("*", "messages-logs-2020.05.15"));
        Assert.assertTrue(StateImpl.matches("messages-logs-*", "messages-logs-2020.05.15"));
        Assert.assertTrue(StateImpl.matches("messages-logs-2020.05*", "messages-logs-2020.05.15"));
        Assert.assertTrue(StateImpl.matches("messages-logs-2020.05.14,messages-logs-2020.05.15", "messages-logs-2020.05.15"));

        Assert.assertFalse(StateImpl.matches("messages-log-*", "messages-logs-2020.05.15"));
        Assert.assertFalse(StateImpl.matches("messages-logs-2020.04*", "messages-logs-2020.05.15"));
        Assert.assertFalse(StateImpl.matches("messages-logs-2020.04.14,messages-logs-2020.04.15", "messages-logs-2020.05.15"));
    }

    @Test
    public void testCompileIndexPattern() {
        testState.getIndex("messages-logs-2020.05.14");
        testState.getIndex("messages-logs-2020.05.15");
        testState.getIndex("messages-logs-2020.05.16");
        testState.getIndex("messages-logs-2020.05.17");

        List<String> result = testState.compileIndexPattern("messages-logs*");
        Assert.assertEquals(4, result.size());
        Assert.assertTrue(result.contains("messages-logs-2020.05.14"));
        Assert.assertTrue(result.contains("messages-logs-2020.05.15"));
        Assert.assertTrue(result.contains("messages-logs-2020.05.16"));
        Assert.assertTrue(result.contains("messages-logs-2020.05.17"));

        result = testState.compileIndexPattern("messages-logs-2020.05.14,messages-logs-2020.05.15");
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains("messages-logs-2020.05.14"));
        Assert.assertTrue(result.contains("messages-logs-2020.05.15"));
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
    public void testFieldComponentIndexWithNoValue() throws ElefanaWrongFieldStatsTypeException {
        final String index2 = TEST_INDEX + "2";
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        CoreFieldStatsJob job = CoreFieldStatsJob.allocate(testState, loadUnloadManager, index2);
        job.addDocument("{\"string\": \"Hello there\", \"long\": null, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ");
        job.run();

        testFieldComponent(TEST_INDEX, "string", String.class);
        testFieldComponent(index2, "string", String.class);
        testFieldComponent(TEST_INDEX, "bool", Boolean.class);
        testFieldComponent(index2, "bool", Boolean.class);
        testFieldComponent(TEST_INDEX, "long", Long.class);
        testFieldComponent(index2, "long", Long.class);
    }

    private <T> void testFieldComponent(String index, String fieldName, Class<T> type) throws ElefanaWrongFieldStatsTypeException {
        FieldStats<T> fieldStats = testState.getFieldStatsTypeChecked(fieldName, type, index);
        FieldComponent fieldComponent = FieldComponent.getFieldComponent(fieldStats, type);
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
    public void testListNestedObjectFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException {
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats<Long> list = testState.getFieldStatsTypeChecked("listObjects.field", Long.class, TEST_INDEX);
        Assert.assertEquals(20, list.getDocumentCount());
        Assert.assertEquals(2 * 20, list.getSumDocumentFrequency());
        Assert.assertEquals(77, list.getMinimumValue().longValue());
        Assert.assertEquals(78, list.getMaximumValue().longValue());
    }

    @Test
    public void testStringFieldStatsReturn() throws ElefanaWrongFieldStatsTypeException{
        submitDocumentNTimes(20, testDocument, TEST_INDEX);

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(20, string.getDocumentCount());
        Assert.assertEquals(40, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", string.getMinimumValue());
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
        Assert.assertEquals("Hello", string.getMinimumValue());
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
        Assert.assertEquals("Hello", string.getMinimumValue());
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
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        List<Thread> threadList = new ArrayList<>();
        for( int i = 0; i < numberOfThreads; i++) {
            threadList.add(new Thread(() -> {
                try {
                    countDownLatch.countDown();
                    countDownLatch.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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

        FieldStats bool = testState.getFieldStats("bool", ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, bool.getDocumentCount());
        Assert.assertEquals(docCount, bool.getSumDocumentFrequency());

        Assert.assertEquals(docCount, testState.getIndex(ImmutableList.of("first", "second")).getMaxDocuments());

        FieldStats string = testState.getFieldStats("string", ImmutableList.of("first", "second"));
        Assert.assertEquals(docCount, string.getDocumentCount());
        Assert.assertEquals(docCount * 2, string.getSumDocumentFrequency());
        Assert.assertEquals("Hello", (String)string.getMinimumValue());
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

        testState.deleteIndex(TEST_INDEX);
        Assert.assertFalse(testState.isIndexLoaded(TEST_INDEX));
    }

    @Test
    public void testLoadIndex() throws ElefanaWrongFieldStatsTypeException {
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
        insert.fields.put("string", new FieldComponent("AA", "s", 9, 9*2, -1, String.class));

        testState.load(insert);

        FieldStats string = testState.getFieldStatsTypeChecked("string", String.class, TEST_INDEX);
        Assert.assertEquals(9 + 20, string.getDocumentCount());
        Assert.assertEquals(9*2 + 20*2, string.getSumDocumentFrequency());
        Assert.assertEquals("AA", string.getMinimumValue());
        Assert.assertEquals("there", string.getMaximumValue());

        assertIndexMaxDocEquals(20 + 10, TEST_INDEX);
    }

    @Test
    public void testLoadIndexConcurrent() throws ElefanaWrongFieldStatsTypeException {
        IndexComponent insert = new IndexComponent(TEST_INDEX, 10);
        insert.fields.put("string", new FieldComponent("AA", "s", 9, 9*2, -1, String.class));

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
        Assert.assertEquals("AA", string.getMinimumValue());
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
        Assert.assertEquals("Hello", string.minValue);
        Assert.assertEquals("there", string.maxValue);
        Assert.assertFalse(testState.isIndexLoaded(TEST_INDEX));
        Assert.assertFalse(testState.isIndexFieldsLoaded(TEST_INDEX));
    }

    @Test(timeout=30000)
    public void testUnloadAndLoadIndexConcurrent() throws ElefanaWrongFieldStatsTypeException {
        final String index = TEST_INDEX + "2";

        List<Thread> threadList = new ArrayList<>();
        final CountDownLatch countDownLatch = new CountDownLatch(60);
        for( int i = 0; i < 50; i++) {
            threadList.add(new Thread(() -> {
                countDownLatch.countDown();
                submitDocumentNTimes(100, testDocument, index);
            }));
        }
        for(int i = 0; i < 10; i++) {
            threadList.add(new Thread(() -> {
                try {
                    countDownLatch.countDown();
                    countDownLatch.await();
                    IndexComponent c = testState.unload(index);
                    if(c == null) {
                        return;
                    }
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
            System.out.println(result.getDocumentCount() + " " + expectedDocCount);
        }
        Assert.assertEquals(expectedDocCount, result.getDocumentCount());
        Assert.assertEquals(expectedSumDocFrequency, result.getSumDocumentFrequency());
        Assert.assertEquals("Hello", result.getMinimumValue());
        Assert.assertEquals("there", result.getMaximumValue());

        assertIndexMaxDocEquals(50*100, index);
    }

    @Test
    public void testFieldUpgradeFromLongToString() throws ElefanaWrongFieldStatsTypeException {
        final String index = "test_index_1";
        final String fieldName = "long";
        final String stringDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": \"upgrade\", \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";

        testState.ensureFieldExists(fieldName, Long.class);
        submitDocumentNTimes(10, testDocument, index);
        submitDocumentNTimes(1, stringDocument, index);
        submitDocumentNTimes(10, testDocument, index);

        final FieldStats<String> result = testState.getFieldStatsTypeChecked(fieldName, String.class, index);
        Assert.assertEquals(21, result.getDocumentCount());
    }

    @Test
    public void testFieldUpgradeFromDoubleToString() throws ElefanaWrongFieldStatsTypeException {
        final String index = "test_index_2";
        final String fieldName = "double";
        final String stringDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": 23, \"double\": \"upgrade\", \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";

        testState.ensureFieldExists(fieldName, Double.class);
        submitDocumentNTimes(10, testDocument, index);
        submitDocumentNTimes(1, stringDocument, index);
        submitDocumentNTimes(10, testDocument, index);

        final FieldStats<String> result = testState.getFieldStatsTypeChecked(fieldName, String.class, index);
        Assert.assertEquals(21, result.getDocumentCount());
    }
}
