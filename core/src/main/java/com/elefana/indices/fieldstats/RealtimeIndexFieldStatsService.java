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

package com.elefana.indices.fieldstats;

import com.elefana.api.RequestExecutor;
import com.elefana.api.indices.GetFieldStatsRequest;
import com.elefana.api.indices.GetFieldStatsResponse;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJob;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJobString;
import com.elefana.indices.fieldstats.job.CoreFieldStatsRemoveIndexJob;
import com.elefana.indices.fieldstats.response.V2FieldStats;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.StateImpl;
import com.elefana.indices.fieldstats.state.field.Field;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.indices.fieldstats.state.field.FieldsImpl;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

@Service
@DependsOn({"nodeSettingsService"})
public class RealtimeIndexFieldStatsService implements IndexFieldStatsService, RequestExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMappingService.class);

    @Autowired
    private Environment environment;
    @Autowired
    private NodeSettingsService nodeSettingsService;
    @Autowired
    private VersionInfoService versionInfoService;
    @Autowired
    private PsqlIndexTemplateService indexTemplateService;
    @Autowired
    private TaskScheduler taskScheduler;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private IndexUtils indexUtils;

    private ExecutorService workerExecutorService;
    private State state;

    @PostConstruct
    public void postConstruct() {
        FieldsImpl.getInstance().registerJsoniterConfig();

        int stateRecords = jdbcTemplate.queryForObject("SELECT COUNT(_state) FROM elefana_fieldstats_state", Integer.class);
        if(stateRecords == 0) {
            state = new StateImpl();
        } else {
            PGobject obj = jdbcTemplate.queryForObject("SELECT _state FROM elefana_fieldstats_state ORDER BY _timestamp DESC LIMIT 1", PGobject.class);
            LOGGER.info(obj.getValue());
            state = JsonIterator.deserialize(obj.getValue(), StateImpl.class);
        }

        final int workerThreadNumber = environment.getProperty("elefana.service.fieldStats.workerThreads", Integer.class, 2);
        workerExecutorService = Executors.newFixedThreadPool(workerThreadNumber);
    }

    @PreDestroy
    public void preDestroy() {
        workerExecutorService.shutdown();
        try {
            workerExecutorService.awaitTermination(30,TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        String serializedState = JsonStream.serialize(state);
        PGobject json = new PGobject();
        json.setType("json");
        try {
            json.setValue(serializedState);
        } catch (SQLException e) {
            LOGGER.error("Invalid JSON", e);
        }
        jdbcTemplate.update("INSERT INTO elefana_fieldstats_state VALUES (?, ?)", System.currentTimeMillis(), json);
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsPost(String indexPattern, String requestBody, boolean clusterLevel) {
        List<String> fields = new ArrayList<>();
        JsonIterator
                .deserialize(requestBody)
                .asMap()
                .get("fields")
                .asList()
                .forEach(s -> fields.add(s.toString()));

        return new RealtimeGetFieldStatsRequest(this, indexPattern, fields, clusterLevel);
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsGet(String indexPattern, String fieldGetParam, boolean clusterLevel) {
        List<String> fields = Arrays.asList(fieldGetParam.split(","));

        return new RealtimeGetFieldStatsRequest(this, indexPattern, fields, clusterLevel);
    }


    @Override
    public GetFieldStatsResponse getFieldStats(String indexPattern, List<String> fields, boolean clusterLevel) {
        GetFieldStatsResponse response = new GetFieldStatsResponse();
        List<String> indices = state.compileIndexPattern(indexPattern);

        setShards(response);
        if(clusterLevel)
            setIndicesClusterLevel(response, indices, fields);
        else
            setIndices(response, indices, fields);

        return response;
    }

    private void setShards(GetFieldStatsResponse response) {
        response.getShards().put("total", 1);
        response.getShards().put("successful", 1);
        response.getShards().put("failed", 0);
    }

    private void setIndicesClusterLevel(GetFieldStatsResponse response, List<String> indices, List<String> fields) {
        indices.forEach(state::stopModificationsOfIndex);
        try {
            response.getIndices().put("_all", getIndexMap(indices, fields));
        } finally {
            indices.forEach(state::resumeModificationsOfIndex);
        }
    }

    private void setIndices(GetFieldStatsResponse response, List<String> indices, List<String> fields){
        for(String index : indices) {
            List<String> indexSingleton = new ArrayList<>();
            indexSingleton.add(index);
            state.stopModificationsOfIndex(index);
            try {
                response.getIndices().put(index, getIndexMap(indexSingleton, fields));
            } finally {
                state.resumeModificationsOfIndex(index);
            }
        }
    }

    private Map<String, Object> getIndexMap(List<String> indices, List<String> fields) {
        Map<String, Object> wrappingMap = new HashMap<>();
        Map<String, Object> fieldsMap = new HashMap<>();
        for(String field : fields) {
            Field f = state.getField(field);
            if(f == null)
                continue;

            FieldStats fs = f.getIndexFieldStats(indices);

            V2FieldStats fieldStats = new V2FieldStats();
            fieldStats.max_doc = state.getIndex(indices).getMaxDocuments();
            fieldStats.density = (long)fs.getDensity(state.getIndex(indices));
            fieldStats.doc_count = fs.getDocumentCount();
            fieldStats.sum_doc_freq = fs.getSumDocumentFrequency();
            fieldStats.sum_total_term_freq = fs.getSumTotalTermFrequency();
            fieldStats.max_value = fs.getMaximumValue();
            fieldStats.max_value_as_string = fs.getMaximumValue().toString();
            fieldStats.min_value = fs.getMinimumValue();
            fieldStats.min_value_as_string = fs.getMinimumValue().toString();

            fieldsMap.put(field, fieldStats);
        }
        wrappingMap.put("fields", fieldsMap);
        return wrappingMap;
    }

    @Override
    public void submitDocument(Any document, String index){
        workerExecutorService.submit(new CoreFieldStatsJob(document, state, index));
    }

    @Override
    public void submitDocument(String document, String index){
        workerExecutorService.submit(new CoreFieldStatsJobString(document, state, index));
    }

    @Override
    public void deleteIndex(String index) {
        workerExecutorService.submit(new CoreFieldStatsRemoveIndexJob(state, index));
    }

    @Override
    public <T> Future<T> submit(Callable<T> request) {
        // Stay on the thread of the request submitter
        FutureTask<T> response = new FutureTask<>(request);
        response.run();
        return response;
    }
}
