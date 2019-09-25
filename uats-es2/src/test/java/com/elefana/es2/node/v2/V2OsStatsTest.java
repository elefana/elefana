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
import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static io.restassured.RestAssured.get;
import static org.hamcrest.Matchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class V2OsStatsTest {

    @Before
    public void setup() {
        RestAssured.baseURI = "http://localhost:9201";
    }

    @Test
    public void testOsStats() {
        Response response = get("/_nodes/stats/os");
        Map<String, ?> nodes = response.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfOsObjIsValid(response, "nodes." + k + ".os");
        });
    }

    @Test
    public void testGlobalStats() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfOsObjIsValid(global, "nodes." + k + ".os");
        });
    }

    @Test
    public void testSingleNodeOsOnly() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats");
            checkIfOsObjIsValid(nodeStats, "nodes." + k + ".os");
        });
    }

    @Test
    public void testSingleNode() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats/os");
            checkIfOsObjIsValid(nodeStats, "nodes." + k + ".os");
        });
    }

    @Test
    public void absenceWhenQueryingJvmStats () {
        Response global = get("/_nodes/stats/jvm");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfOsObjIsAbsent(global, "nodes." + k + ".os");
        });
    }

    @Test
    public void nodesEmptyWhenQueryingNonExistentID() {
        get("/_nodes/thisNodeDoesNotExist/stats").then().body("nodes.isEmpty()", is(true));
        get("/_nodes/thisNodeDoesNotExist/stats/os").then().body("nodes.isEmpty()", is(true));
    }

    private void checkIfOsObjExists(Response response, String pathToOsObj) {
        response.then()
                .statusCode(200)
                .body(pathToOsObj, is(notNullValue()))
                .body(pathToOsObj, is(not(empty())));
    }

    private void checkIfOsObjIsValid(Response response, String pathToOsObj) {
        checkIfOsObjExists(response, pathToOsObj);
        response.then()
                .body(pathToOsObj + ".timestamp", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".cpu_percent", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".load_average", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".mem.total_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".mem.free_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".mem.used_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".mem.free_percent", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".mem.used_percent", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".swap.total_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".swap.free_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))))
                .body(pathToOsObj + ".swap.used_in_bytes", is(either(instanceOf(Long.class)).or(instanceOf(Integer.class))));
    }

    private void checkIfOsObjIsAbsent(Response response, String pathToOsObj) {
        response.then()
                .log().all(true)
                .statusCode(200)
                .body(pathToOsObj, is(nullValue()));
    }
}
