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
import com.elefana.indices.fieldstats.MasterLoadUnloadManager;
import com.elefana.indices.fieldstats.state.field.ElefanaWrongFieldStatsTypeException;
import com.elefana.indices.fieldstats.state.index.Index;
import com.elefana.indices.fieldstats.state.index.IndexComponent;
import com.elefana.indices.fieldstats.state.index.IndexImpl;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MasterLoadUnloadManagerTest {

    private static final String TEST_INDEX = "testIndex";
    private static final String TEST_INDEX_TWO = "otherIndex";
    private static final String TEST_INDEX_DELETE = "deleteIndex";

    private State testState;
    private JdbcTemplate jdbcTemplate;
    private LoadUnloadManager loadUnloadManager;

    @Before
    public void before() {
        testState = mock(StateImpl.class);
        jdbcTemplate = mock(JdbcTemplate.class);

        when(jdbcTemplate.queryForList(eq("SELECT _indexname FROM elefana_field_stats_index"), eq(String.class))).thenReturn(ImmutableList.of(TEST_INDEX, TEST_INDEX_TWO, TEST_INDEX_DELETE));
        when(jdbcTemplate.queryForObject(eq("SELECT * FROM elefana_field_stats_index WHERE _indexname = ?"), any(RowMapper.class), eq(TEST_INDEX)))
                .thenReturn(new IndexComponent(TEST_INDEX, 20));
        when(jdbcTemplate.queryForObject(eq("SELECT * FROM elefana_field_stats_index WHERE _indexname = ?"), any(RowMapper.class), eq(TEST_INDEX_TWO)))
                .thenReturn(new IndexComponent(TEST_INDEX_TWO, 10));
        when(jdbcTemplate.queryForObject(eq("SELECT * FROM elefana_field_stats_index WHERE _indexname = ?"), any(RowMapper.class), eq(TEST_INDEX_DELETE)))
                .thenReturn(null);

        Index a = new IndexImpl(), b = new IndexImpl();
        when(testState.getIndex(eq(TEST_INDEX))).thenReturn(a);
        when(testState.getIndex(eq(TEST_INDEX_TWO))).thenReturn(b);

        loadUnloadManager = new MasterLoadUnloadManager(jdbcTemplate, testState, true,10, 5);
    }

    @After
    public void teardown() {
        loadUnloadManager.shutdown();
    }

    @Test
    public void testUnloadUnusedIndices() {
        loadUnloadManager.shutdown();
        loadUnloadManager = new MasterLoadUnloadManager(jdbcTemplate, testState, true,0, 0);

        loadUnloadManager.ensureIndicesLoaded(TEST_INDEX);

        for(int i = 0; i < 60000; i++) {
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {}

            if(!loadUnloadManager.isIndexLoaded(TEST_INDEX)) {
                break;
            }
        }
        Assert.assertFalse(loadUnloadManager.isIndexLoaded(TEST_INDEX));
    }

    @Test
    public void testConcurrent() throws ElefanaWrongFieldStatsTypeException {
        List<Thread> threads = new ArrayList<>();
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        for(int i = 0; i < 5; i++){
            threads.add(new Thread(() -> {
                countDownLatch.countDown();
                loadUnloadManager.ensureIndicesLoaded(TEST_INDEX);
            }));
        }
        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        verify(testState, times(1)).load(any());
        Assert.assertTrue(loadUnloadManager.isIndexLoaded(TEST_INDEX));
    }

    @Test
    public void testConcurrentMultipleIndices() throws ElefanaWrongFieldStatsTypeException {
        List<Thread> threads = new ArrayList<>();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for(int i = 0; i < 5; i++){
            threads.add(new Thread(() -> {
                countDownLatch.countDown();
                loadUnloadManager.ensureIndicesLoaded(TEST_INDEX);
            }));
        }
        for(int i = 0; i < 5; i++){
            threads.add(new Thread(() -> {
                countDownLatch.countDown();
                loadUnloadManager.ensureIndicesLoaded(TEST_INDEX_TWO);
            }));
        }
        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        verify(testState, times(1)).load(argThat(ic -> ic.name.equals(TEST_INDEX)));

        verify(testState, times(1)).load(argThat(ic -> ic.name.equals(TEST_INDEX_TWO)));
        Assert.assertTrue(loadUnloadManager.isIndexLoaded(TEST_INDEX));
        Assert.assertTrue(loadUnloadManager.isIndexLoaded(TEST_INDEX_TWO));
    }

    @Test
    public void testDeleteMissingIndex() throws ElefanaWrongFieldStatsTypeException {
        loadUnloadManager.deleteIndex(TEST_INDEX_DELETE);
        loadUnloadManager.ensureIndicesLoaded(TEST_INDEX_DELETE);

        verify(testState, times(0)).load(any());
        verify(testState, times(1)).deleteIndex(anyString());
    }

}
