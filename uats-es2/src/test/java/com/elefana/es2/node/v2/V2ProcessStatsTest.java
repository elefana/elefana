package com.elefana.es2.node.v2;

import com.elefana.ElefanaApplication;
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
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class V2ProcessStatsTest {

    private static final Matcher<Object> isIntegralNumber = is(either(instanceOf(Long.class)).or(instanceOf(Integer.class)));

    @Before
    public void setup() {
        RestAssured.baseURI = "http://localhost:9201";
    }

    @Test
    public void testProcessStats() {
        Response response = get("/_nodes/stats/process");
        Map<String, ?> nodes = response.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfProcessObjIsValid(response, "nodes." + k + ".process");
        });
    }

    @Test
    public void testGlobalStats() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfProcessObjIsValid(global, "nodes." + k + ".process");
        });
    }

    @Test
    public void testSingleNodeProcessOnly() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats");
            checkIfProcessObjIsValid(nodeStats, "nodes." + k + ".process");
        });
    }

    @Test
    public void testSingleNode() {
        Response global = get("/_nodes/stats");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            Response nodeStats = get("/_nodes/" + k + "/stats/process");
            checkIfProcessObjIsValid(nodeStats, "nodes." + k + ".process");
        });
    }

    @Test
    public void absenceWhenQueryingJvmStats () {
        Response global = get("/_nodes/stats/jvm");
        Map<String, ?> nodes = global.path("nodes");
        nodes.forEach((k, v) -> {
            checkIfProcessObjIsAbsent(global, "nodes." + k + ".process");
        });
    }

    private void checkIfProcessObjExists(Response response, String pathToProcessObj) {
        response.then()
                .statusCode(200)
                .body(pathToProcessObj, is(notNullValue()))
                .body(pathToProcessObj, is(not(empty())));
    }

    private void checkIfProcessObjIsValid(Response response, String pathToProcessObj) {
        checkIfProcessObjExists(response, pathToProcessObj);
        response.then()
                .body(pathToProcessObj + ".timestamp", isIntegralNumber)
                .body(pathToProcessObj + ".open_file_descriptors", isIntegralNumber)
                .body(pathToProcessObj + ".max_file_descriptors", isIntegralNumber)
                .body(pathToProcessObj + ".cpu.percent", isIntegralNumber)
                .body(pathToProcessObj + ".cpu.total_in_millis", isIntegralNumber)
                .body(pathToProcessObj + ".mem.total_virtual_in_bytes", isIntegralNumber);
    }

    private void checkIfProcessObjIsAbsent(Response response, String pathToProcessObj) {
        response.then()
                .log().all(true)
                .statusCode(200)
                .body(pathToProcessObj, is(nullValue()));
    }
}
