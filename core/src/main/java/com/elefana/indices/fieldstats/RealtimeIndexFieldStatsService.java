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
import com.elefana.api.exception.NoSuchApiException;
import com.elefana.api.indices.GetFieldStatsRequest;
import com.elefana.api.indices.GetFieldStatsResponse;
import com.elefana.document.BulkIndexOperation;
import com.elefana.indices.IndexFieldMappingService;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJob;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJobString;
import com.elefana.indices.fieldstats.response.V2FieldStats;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.StateImpl;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.indices.psql.PsqlIndexTemplateService;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.jsoniter.JsonIterator;
import com.jsoniter.spi.JsonException;
import io.netty.handler.codec.http.HttpMethod;
import com.jsoniter.any.Any;
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

    private ExecutorService workerExecutorService, requestExecutorService;
    private State state;
    private LoadUnloadManager loadUnloadManager;

    @PostConstruct
    public void postConstruct() {
        state = new StateImpl();

        long indexTtlMinutes = environment.getProperty("elefana.service.fieldStats.cache.ttlMinutes", Integer.class, 10);
        loadUnloadManager = new LoadUnloadManager(jdbcTemplate, state, indexTtlMinutes);

        final int workerThreadNumber = environment.getProperty("elefana.service.fieldStats.workerThreads", Integer.class, 2);
        workerExecutorService = Executors.newFixedThreadPool(workerThreadNumber);

        final int requestThreadNumber = environment.getProperty("elefana.service.fieldStats.requestThreads", Integer.class, 1);
        requestExecutorService = Executors.newFixedThreadPool(requestThreadNumber);
    }

    @PreDestroy
    public void preDestroy() {
        requestExecutorService.shutdown();
        workerExecutorService.shutdown();
        loadUnloadManager.shutdown();

        try {
            requestExecutorService.awaitTermination(30, TimeUnit.SECONDS);
            workerExecutorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        loadUnloadManager.unloadAll();
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsPost(String indexPattern, String requestBody, boolean clusterLevel) throws NoSuchApiException {
        try {
            List<String> fields = new ArrayList<>();

            JsonIterator
                    .deserialize(requestBody)
                    .asMap()
                    .get("fields")
                    .asList()
                    .forEach(s -> fields.add(s.toString()));

            if (fields.isEmpty()) {
                throw new NoSuchApiException(HttpMethod.POST, "no fields specified in request body");
            }

            return new RealtimeGetFieldStatsRequest(this, indexPattern, fields, clusterLevel);
        } catch (JsonException e) {
            throw new NoSuchApiException(HttpMethod.POST, "invalid request body");
        }
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsGet(String indexPattern, String fieldGetParam, boolean clusterLevel) {
        List<String> fields = Arrays.asList(fieldGetParam.split(","));

        return new RealtimeGetFieldStatsRequest(this, indexPattern, fields, clusterLevel);
    }


    @Override
    public GetFieldStatsResponse getFieldStats(String indexPattern, List<String> fields, boolean clusterLevel) {
        GetFieldStatsResponse response = new GetFieldStatsResponse();
        loadUnloadManager.ensureIndexIsLoaded(indexPattern);
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
            state.stopModificationsOfIndex(index);
            try {
                response.getIndices().put(index, getIndexMap(Collections.singletonList(index), fields));
            } finally {
                state.resumeModificationsOfIndex(index);
            }
        }
    }

    private Map<String, Object> getIndexMap(List<String> indices, List<String> fields) {
        Map<String, Object> wrappingMap = new HashMap<>();
        Map<String, Object> fieldsMap = new HashMap<>();
        for(String field : fields) {
            FieldStats fs = state.getFieldStats(field, indices);
            if(fs == null)
                continue;

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
        workerExecutorService.submit(new CoreFieldStatsJob(document, state, loadUnloadManager, index));
    }

    @Override
    public void submitDocuments(List<BulkIndexOperation> documents) {
        for(BulkIndexOperation i: documents) {
            workerExecutorService.submit(new CoreFieldStatsJobString(i.getSource(), state, loadUnloadManager, i.getIndex()));
        }
    }

    @Override
    public void submitDocument(String document, String index){
        workerExecutorService.submit(new CoreFieldStatsJobString(document, state, loadUnloadManager, index));
    }

    @Override
    public <T> Future<T> submit(Callable<T> request) {
        return requestExecutorService.submit(request);
    }
}
