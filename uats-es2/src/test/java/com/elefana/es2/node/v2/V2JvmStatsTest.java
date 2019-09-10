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

package com.elefana.es2.node.v2;

import com.elefana.ElefanaApplication;
import com.elefana.node.JvmStats;
import com.elefana.node.v2.V2JvmStats;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static io.restassured.RestAssured.get;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.nullValue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class V2JvmStatsTest {

    private static final Matcher<Object> isIntegralNumber = is(either(instanceOf(Long.class)).or(instanceOf(Integer.class)));

    @Before
    public void setup() {
        RestAssured.baseURI = "http://localhost:9201";
    }

    @Test
    public void testJvmStats() {
        Response response = get("/_nodes/stats/jvm");
        Map<String, ?> nodes = response.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfJvmObjIsValid(response, "nodes.'" + k + "'.jvm");
        });
    }

    @Test
    public void testGlobalStats() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfJvmObjIsValid(global, "nodes.'" + k + "'.jvm");
        });
    }

    @Test
    public void testSingleNodeJvmOnly() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats");
            checkIfJvmObjIsValid(nodeStats, "nodes.'" + k + "'.jvm");
        });
    }

    @Test
    public void testSingleNode() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats/jvm");
            checkIfJvmObjIsValid(nodeStats, "nodes.'" + k + "'.jvm");
        });
    }

    @Test
    public void absenceWhenQueryingOsStats () {
        Response global = get("/_nodes/stats/os");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfJvmObjIsAbsent(global, "nodes.'" + k + "'.jvm");
        });
    }

    private void checkIfJvmObjIsAbsent(Response response, String pathToJvmObj) {
        response.then()
                .statusCode(200)
                .body(pathToJvmObj, is(nullValue()));
    }

    private void checkIfJvmObjIsValid(Response response, String pathToJvmObj) {
        checkIfJvmObjExists(response, pathToJvmObj);
        checkJvmObjStaticProperties(response, pathToJvmObj);
        checkJvmObjMemoryPools(response, pathToJvmObj);
        checkJvmObjGarbageCollectors(response, pathToJvmObj);
        checkJvmObjBufferPools(response, pathToJvmObj);
    }

    private void checkIfJvmObjExists(Response response, String pathToJvmObj) {
        response.then()
                .statusCode(200)
                .body(pathToJvmObj, is(notNullValue()))
                .body(pathToJvmObj, is(not(empty())));
    }

    private void checkJvmObjStaticProperties(Response response, String pathToJvmObj) {
        response.then()
                .body(pathToJvmObj + ".timestamp", isIntegralNumber)
                .body(pathToJvmObj + ".uptime_in_millis", isIntegralNumber)
                .body(pathToJvmObj + ".mem.heap_used_in_bytes", isIntegralNumber)
                .body(pathToJvmObj + ".mem.heap_used_percent", isIntegralNumber)
                .body(pathToJvmObj + ".mem.heap_committed_in_bytes", isIntegralNumber)
                .body(pathToJvmObj + ".mem.heap_max_in_bytes", isIntegralNumber)
                .body(pathToJvmObj + ".mem.non_heap_used_in_bytes", isIntegralNumber)
                .body(pathToJvmObj + ".mem.non_heap_committed_in_bytes", isIntegralNumber)
                .body(pathToJvmObj + ".threads.count", isIntegralNumber)
                .body(pathToJvmObj + ".threads.peak_count", isIntegralNumber)
                .body(pathToJvmObj + ".classes.current_loaded_count", isIntegralNumber)
                .body(pathToJvmObj + ".classes.total_loaded_count", isIntegralNumber)
                .body(pathToJvmObj + ".classes.total_unloaded_count", isIntegralNumber);
    }

    private void checkJvmObjMemoryPools(Response response, String pathToJvmObj) {
        String pathToMemPools = pathToJvmObj + ".mem.pools";
        Map<String, ?> memPools = response.path(pathToMemPools);
        memPools.forEach((key, value) -> {
            checkIfMemoryPoolObjIsValid(response, pathToMemPools + ".'" + key + "'");
        });
    }

    private void checkIfMemoryPoolObjIsValid(Response response, String pathToMemPool) {
        response.then()
                .body(pathToMemPool + ".used_in_bytes", isIntegralNumber)
                .body(pathToMemPool + ".max_in_bytes", isIntegralNumber)
                .body(pathToMemPool + ".peak_used_in_bytes", isIntegralNumber)
                .body(pathToMemPool + ".peak_max_in_bytes", isIntegralNumber);
    }

    private void checkJvmObjGarbageCollectors(Response response, String pathToJvmObj) {
        String pathToGarbageCollectors = pathToJvmObj + ".gc.collectors";
        Map<String, ?> garbageCollectors = response.path(pathToGarbageCollectors);
        garbageCollectors.forEach((key, value) -> {
            checkIfGarbageCollectorObjIsValid(response, pathToGarbageCollectors + ".'" + key + "'");
        });
    }

    private void checkIfGarbageCollectorObjIsValid(Response response, String pathToGarbageCollector) {
        response.then()
                .body(pathToGarbageCollector + ".collection_count", isIntegralNumber)
                .body(pathToGarbageCollector + ".collection_time_in_millis", isIntegralNumber);
    }

    private void checkJvmObjBufferPools(Response response, String pathToJvmObj) {
        String pathToBufferPools = pathToJvmObj + ".buffer_pools";
        Map<String, ?> bufferPools = response.path(pathToBufferPools);
        bufferPools.forEach((key, value) -> {
            checkIfBufferPoolObjIsValid(response, pathToBufferPools + ".'" + key + "'");
        });
    }

    private void checkIfBufferPoolObjIsValid(Response response, String pathToBufferPool) {
        response.then()
                .body(pathToBufferPool + ".count", isIntegralNumber)
                .body(pathToBufferPool + ".used_in_bytes", isIntegralNumber)
                .body(pathToBufferPool + ".total_capacity_in_bytes", isIntegralNumber);
    }
}
