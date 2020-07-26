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
import com.elefana.api.indices.*;
import com.elefana.api.json.JsonUtils;
import com.elefana.api.util.PooledStringBuilder;
import com.elefana.document.BulkIndexOperation;
import com.elefana.indices.IndexTemplateService;
import com.elefana.indices.fieldstats.job.CoreFieldStatsJob;
import com.elefana.indices.fieldstats.job.CoreFieldStatsRemoveIndexJob;
import com.elefana.indices.fieldstats.response.V2FieldStats;
import com.elefana.indices.fieldstats.state.State;
import com.elefana.indices.fieldstats.state.StateImpl;
import com.elefana.indices.fieldstats.state.field.FieldStats;
import com.elefana.node.NodeSettingsService;
import com.elefana.node.VersionInfoService;
import com.elefana.util.IndexUtils;
import com.elefana.util.NamedThreadFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.handler.codec.http.HttpMethod;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Service
@DependsOn({"nodeSettingsService"})
public class CoreIndexFieldStatsService implements IndexFieldStatsService, RequestExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreIndexFieldStatsService.class);

    @Autowired
    protected Environment environment;
    @Autowired
    protected NodeSettingsService nodeSettingsService;
    @Autowired
    protected VersionInfoService versionInfoService;
    @Autowired
    protected IndexTemplateService indexTemplateService;
    @Autowired
    protected TaskScheduler taskScheduler;
    @Autowired
    protected JdbcTemplate jdbcTemplate;
    @Autowired
    protected IndexUtils indexUtils;

    protected ExecutorService requestExecutorService;
    protected State state;
    protected LoadUnloadManager loadUnloadManager;

    private final Cache<String, Boolean> isBooleanFieldCache = CacheBuilder.newBuilder().maximumSize(2048).expireAfterAccess(Duration.ofHours(1)).build();
    private final Cache<String, Boolean> isDateFieldCache = CacheBuilder.newBuilder().maximumSize(2048).expireAfterAccess(Duration.ofHours(1)).build();
    private final Cache<String, Boolean> isDoubleFieldCache = CacheBuilder.newBuilder().maximumSize(2048).expireAfterAccess(Duration.ofHours(1)).build();
    private final Cache<String, Boolean> isLongFieldCache = CacheBuilder.newBuilder().maximumSize(2048).expireAfterAccess(Duration.ofHours(1)).build();
    private final Cache<String, Boolean> isStringFieldCache = CacheBuilder.newBuilder().maximumSize(2048).expireAfterAccess(Duration.ofHours(1)).build();

    @PostConstruct
    public void postConstruct() {
        state = createState(environment);

        long indexTtlMinutes = environment.getProperty("elefana.service.fieldStats.cache.ttlMinutes", Integer.class, 60);
        long indexSnapshotMinutes = environment.getProperty("elefana.service.fieldStats.cache.snapshotMinutes", Integer.class, 5);
        loadUnloadManager = createLoadUnloadManager(nodeSettingsService.isMasterNode(), indexTtlMinutes, indexSnapshotMinutes);

        final int requestThreadNumber = environment.getProperty("elefana.service.fieldStats.requestThreads", Integer.class, 2);
        requestExecutorService = Executors.newFixedThreadPool(requestThreadNumber, new NamedThreadFactory("elefana-fieldStatsService-requestExecutor"));
    }

    @PreDestroy
    public void preDestroy() {
        requestExecutorService.shutdown();

        try {
            requestExecutorService.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        if (nodeSettingsService.isMasterNode()) {
            loadUnloadManager.unloadAll();
            loadUnloadManager.shutdown();
        }
    }

    protected State createState(Environment environment) {
        return new StateImpl(environment);
    }

    protected LoadUnloadManager createLoadUnloadManager(boolean master, long indexTtlMinutes, long indexSnapshotMinutes) {
        if(master) {
            return new MasterLoadUnloadManager(jdbcTemplate, state, master, indexTtlMinutes, indexSnapshotMinutes);
        }
        return new NoopLoadUnloadManager();
    }

    protected void ensureIndicesLoaded(String indexPattern) {
        loadUnloadManager.ensureIndicesLoaded(indexPattern);
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsPost(String indexPattern, PooledStringBuilder requestBody, boolean clusterLevel) throws NoSuchApiException {
        try {
            List<String> fields = new ArrayList<>();

            final JsonNode jsonNode = JsonUtils.extractJsonNode(requestBody, "fields");
            for(int i = 0; i < jsonNode.size(); i++) {
                fields.add(jsonNode.get(i).textValue());
            }
            if (fields.isEmpty()) {
                throw new NoSuchApiException(HttpMethod.POST, "No fields specified in request body");
            }

            return new RealtimeGetFieldStatsRequest(this, indexPattern, fields, clusterLevel);
        } catch (Exception e) {
            throw new NoSuchApiException(HttpMethod.POST, "Invalid request body");
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
        ensureIndicesLoaded(indexPattern);
        List<String> indices = state.compileIndexPattern(indexPattern);

        setResponseShardInfo(response);

        if (clusterLevel) {
            getFieldStatsClusterLevel(response, indices, fields);
        } else {
            getFieldStatsIndicesLevel(response, indices, fields);
        }
        return response;
    }

    @Override
    public GetFieldNamesRequest prepareGetFieldNames(String indexPattern) {
        return prepareGetFieldNames(indexPattern, "*");
    }

    @Override
    public GetFieldNamesRequest prepareGetFieldNames(String indexPattern, String typePattern) {
        return new RealtimeIndexFieldNamesRequest(this, indexPattern, typePattern);
    }

    @Override
    public GetFieldNamesResponse getFieldNames(String indexPattern, String typePattern) {
        final GetFieldNamesResponse response = new GetFieldNamesResponse();
        ensureIndicesLoaded(indexPattern);
        List<String> indices = state.compileIndexPattern(indexPattern);

        for (String index : indices) {
            state.getFieldNames(response.getFieldNames(), index);
        }
        return response;
    }

    @Override
    public boolean isBooleanField(final String index, final String field) {
        try {
            return isBooleanFieldCache.get(field, () -> {
                ensureIndicesLoaded(index);
                return state.getFieldStats(field, index).getFieldClass().equals(Boolean.class);
            });
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean isDateField(final String index, final String field) {
        try {
            return isDateFieldCache.get(field, () -> {
                ensureIndicesLoaded(index);
                return state.getFieldStats(field, index).getFieldClass().equals(Date.class);
            });
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean isDoubleField(final String index, final String field) {
        try {
            return isDoubleFieldCache.get(field, () -> {
                ensureIndicesLoaded(index);
                return state.getFieldStats(field, index).getFieldClass().equals(Double.class);
            });
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean isLongField(final String index, final String field) {
        try {
            return isLongFieldCache.get(field, () -> {
                ensureIndicesLoaded(index);
                return state.getFieldStats(field, index).getFieldClass().equals(Long.class);
            });
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean isStringField(final String index, final String field) {
        try {
            return isStringFieldCache.get(field, () -> {
                ensureIndicesLoaded(index);
                return state.getFieldStats(field, index).getFieldClass().equals(String.class);
            });
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    private void setResponseShardInfo(GetFieldStatsResponse response) {
        response.getShards().put("total", 1);
        response.getShards().put("successful", 1);
        response.getShards().put("failed", 0);
    }

    private void getFieldStatsClusterLevel(GetFieldStatsResponse response, List<String> indices, List<String> fields) {
        response.getIndices().put("_all", getFieldStatsMap(indices, fields));
    }

    private void getFieldStatsIndicesLevel(GetFieldStatsResponse response, List<String> indices, List<String> fields) {
        for (String index : indices) {
            response.getIndices().put(index, getFieldStatsMap(Collections.singletonList(index), fields));
        }
    }

    private Map<String, Object> getFieldStatsMap(List<String> indices, List<String> fields) {
        final Map<String, Object> wrappingMap = new HashMap<>();
        final Map<String, Object> fieldsMap = new HashMap<>();
        for (String field : fields) {
            FieldStats fs = state.getFieldStats(field, indices);
            if (fs == null) {
                continue;
            }

            V2FieldStats fieldStats = new V2FieldStats();
            fieldStats.max_doc = state.getIndex(indices).getMaxDocuments();
            fieldStats.density = (long) fs.getDensity(state.getIndex(indices));
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
    public void submitDocuments(List<BulkIndexOperation> documents, int from, int size) {
        CoreFieldStatsJob fieldStatsJob = null;

        for (int i = from; i < from + size && i < documents.size(); i++) {
            BulkIndexOperation operation = documents.get(i);

            if (isStatsDisabled(operation.getIndex())) {
                operation.release();
                continue;
            }

            if(fieldStatsJob == null) {
                ensureIndicesLoaded(operation.getIndex());
                fieldStatsJob = CoreFieldStatsJob.allocate(state, loadUnloadManager, operation.getIndex());
            } else if(!fieldStatsJob.getIndexName().equals(operation.getIndex())) {
                fieldStatsJob.run();

                ensureIndicesLoaded(operation.getIndex());
                fieldStatsJob = CoreFieldStatsJob.allocate(state, loadUnloadManager, operation.getIndex());
            }

            fieldStatsJob.addDocument(operation);
        }

        if(fieldStatsJob != null) {
            fieldStatsJob.run();
        }
    }

    @Override
    public void deleteIndex(String index) {
        new CoreFieldStatsRemoveIndexJob(state, loadUnloadManager, index).run();
    }

    @Override
    public void submitDocument(String document, String index) {
        if (isStatsDisabled(index)) {
            return;
        }
        ensureIndicesLoaded(index);
        final CoreFieldStatsJob fieldStatsJob = CoreFieldStatsJob.allocate(state, loadUnloadManager, index);
        fieldStatsJob.addDocument(document);
        fieldStatsJob.run();
    }

    @Override
    public void submitDocument(PooledStringBuilder document, String index) {
        if (isStatsDisabled(index)) {
            return;
        }
        ensureIndicesLoaded(index);
        final CoreFieldStatsJob fieldStatsJob = CoreFieldStatsJob.allocate(state, loadUnloadManager, index);
        fieldStatsJob.addDocument(document);
        fieldStatsJob.run();
    }

    @Override
    public <T> Future<T> submit(Callable<T> request) {
        return requestExecutorService.submit(request);
    }

    private boolean isStatsDisabled(String index) {
        try {
            final IndexTemplate indexTemplate = indexTemplateService.getIndexTemplateForIndex(index);
            if(indexTemplate == null) {
                return false;
            }
            if(indexTemplate.getStorage() == null) {
                return false;
            }
            return !indexTemplate.getStorage().isFieldStatsEnabled();
        } catch (Exception e) {
            return false;
        }
    }
}