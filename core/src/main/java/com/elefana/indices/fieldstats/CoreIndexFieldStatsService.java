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
import com.elefana.util.ThreadPriorities;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
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
    private static final int TABLES_FROM_DB_BATCH_SIZE = 6;

    @Autowired
    protected Environment environment;
    @Autowired
    protected NodeSettingsService nodeSettingsService;
    @Autowired
    protected VersionInfoService versionInfoService;
    @Autowired
    protected IndexTemplateService indexTemplateService;
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

    private LoadingCache<String, Set<String>> fieldNamesCache;

    @PostConstruct
    public void postConstruct() {
        fieldNamesCache = CacheBuilder.newBuilder().
                maximumSize(environment.getProperty("elefana.service.field.cache.names.expire.size", Integer.class, 512)).
                expireAfterWrite(Duration.ofSeconds(environment.getProperty("elefana.service.field.cache.names.expire.time", Long.class, 3600L))).build(new CacheLoader<String, Set<String>>() {
            @Override
            public Set<String> load(String key) throws Exception {
                return getFieldNamesFromDatabase(key);
            }
        });

        state = createState(environment);

        long indexTtlMinutes = environment.getProperty("elefana.service.fieldStats.cache.ttlMinutes", Integer.class, 60);
        long indexSnapshotMinutes = environment.getProperty("elefana.service.fieldStats.cache.snapshotMinutes", Integer.class, 5);
        loadUnloadManager = createLoadUnloadManager(nodeSettingsService.isMasterNode(), indexTtlMinutes, indexSnapshotMinutes);

        final int requestThreadNumber = environment.getProperty("elefana.service.fieldStats.requestThreads", Integer.class, 2);
        requestExecutorService = Executors.newFixedThreadPool(requestThreadNumber, new NamedThreadFactory(
                "elefana-fieldStatsService-requestExecutor", ThreadPriorities.FIELD_STATS_SERVICE));
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
    public GetFieldStatsRequest prepareGetFieldStatsPost(ChannelHandlerContext context, String indexPattern, PooledStringBuilder requestBody, boolean clusterLevel) throws NoSuchApiException {
        try {
            List<String> fields = new ArrayList<>();

            final JsonNode jsonNode = JsonUtils.extractJsonNode(requestBody, "fields");
            for(int i = 0; i < jsonNode.size(); i++) {
                fields.add(jsonNode.get(i).textValue());
            }
            if (fields.isEmpty()) {
                throw new NoSuchApiException(HttpMethod.POST, "No fields specified in request body");
            }

            return new RealtimeGetFieldStatsRequest(this, context, indexPattern, fields, clusterLevel);
        } catch (Exception e) {
            throw new NoSuchApiException(HttpMethod.POST, "Invalid request body");
        }
    }

    @Override
    public GetFieldStatsRequest prepareGetFieldStatsGet(ChannelHandlerContext context, String indexPattern, String fieldGetParam, boolean clusterLevel) {
        List<String> fields = Arrays.asList(fieldGetParam.split(","));

        return new RealtimeGetFieldStatsRequest(this, context, indexPattern, fields, clusterLevel);
    }

    @Override
    public GetFieldStatsResponse getFieldStats(ChannelHandlerContext context, String indexPattern, List<String> fields, boolean clusterLevel) {
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
    public GetFieldNamesRequest prepareGetFieldNames(ChannelHandlerContext context, String indexPattern) {
        return prepareGetFieldNames(context, indexPattern, "*");
    }

    @Override
    public GetFieldNamesRequest prepareGetFieldNames(ChannelHandlerContext context, String indexPattern, String typePattern) {
        return new RealtimeIndexFieldNamesRequest(this, context, indexPattern, typePattern);
    }

    @Override
    public GetFieldNamesResponse getFieldNames(ChannelHandlerContext context, String indexPattern, String typePattern) {
        final GetFieldNamesResponse response = new GetFieldNamesResponse();
        try {
            final Set<String> cachedFieldNames = fieldNamesCache.get(indexPattern);
            if(cachedFieldNames.isEmpty()) {
                fieldNamesCache.invalidate(indexPattern);
                response.getFieldNames().addAll(getFieldNamesFromDatabase(indexPattern));
            } else {
                response.getFieldNames().addAll(cachedFieldNames);
            }
        } catch (ExecutionException e) {
            response.getFieldNames().addAll(getFieldNamesFromDatabase(indexPattern));
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

    private Set<String> getFieldNamesFromDatabase(String indexPattern) {
        final Set<String> result = new HashSet<String>();
        try {
            final List<String> indices = loadUnloadManager.compileIndexPattern(indexPattern);
            if(indices.isEmpty()) {
                return result;
            }

            for(int i = 0; i < indices.size(); i += TABLES_FROM_DB_BATCH_SIZE) {
                getFieldNamesFromDatabase(result, indices, i, TABLES_FROM_DB_BATCH_SIZE);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return result;
    }

    private void getFieldNamesFromDatabase(final Set<String> result, final List<String> indices,
                                           int from, int length) {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT DISTINCT _fieldname FROM elefana_field_stats_fieldstats WHERE _indexname IN (");
        for(int i = from; i < indices.size() && i < from + length; i++) {
            final String index = indices.get(i);
            queryBuilder.append('\'');
            queryBuilder.append(index);
            queryBuilder.append('\'');
            if(i < indices.size() - 1 && i < (from + length) - 1) {
                queryBuilder.append(',');
            }
        }
        queryBuilder.append(");");

        final String query = queryBuilder.toString();
        LOGGER.info(query);
        final SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        while(sqlRowSet.next()) {
            result.add(sqlRowSet.getString("_fieldname"));
        }
    }
}