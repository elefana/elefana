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

package com.elefana.es2.indices.fieldstats;

import com.elefana.ElefanaApplication;
import com.elefana.TestUtils;
import com.elefana.document.psql.PsqlBulkIngestService;
import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.config.EncoderConfig;
import io.restassured.response.ValidatableResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class FieldStatsServiceTest {
    private static final long BULK_INDEX_TIMEOUT = 30000L;

    private static final int RANDOM_SEED = 12947358;
    private static final Random RANDOM = new Random(RANDOM_SEED);

    private String testDocument;
    private String testDocumentNoBool;

    @Before
    public void setup() {
        RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig().defaultContentCharset("UTF-8")).
                decoderConfig(DecoderConfig.decoderConfig().defaultContentCharset("UTF-8"));
        RestAssured.baseURI = "http://localhost:9201";

        testDocument = "{ \"bool\": false, \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] }";
        testDocumentNoBool = "{ \"string\": \"Hello there\", \"long\": 23, \"double\": 2.4, \"obj\": { \"bic\": \"EASYATW1\", \"iban\": \"AT12 4321\" }, \"list\": [3,4,5,6,6,4,4,2] } ";
    }

    @Test(timeout=30000)
    public void testFieldStatsGeneration() {
        final String index = UUID.randomUUID().toString();
        final String type = "test";

        final int totalDocuments = 100;
        final int totalEmptyFieldDocuments = 50;

        submitDocumentNTimes(totalEmptyFieldDocuments, testDocumentNoBool, index, type);
        submitDocumentNTimes(totalDocuments - totalEmptyFieldDocuments, testDocument, index, type);

        getFieldStats(index, index,"string", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.doc_count", equalTo(totalDocuments));

        getFieldStats(index, index,"bool", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.bool.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.bool.doc_count", equalTo(totalEmptyFieldDocuments))
                .body("indices." + index + ".fields.bool.max_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.max_value_as_string", isA(String.class))
                .body("indices." + index + ".fields.bool.min_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.min_value_as_string", isA(String.class));
    }

    @Test(timeout=30000)
    public void testDeleteIndex() {
        final String index = UUID.randomUUID().toString();
        final String type = "test";

        int documentsBeforeDelete = 10;
        int documentsAfterDelete = 20;

        submitDocumentNTimes(documentsBeforeDelete, testDocument, index, type);
        given()
                .when()
                .delete("/" + index)
                .then()
                .statusCode(200);
        submitDocumentNTimes(documentsAfterDelete, testDocument, index, type);

        getFieldStats(index,  index,"string", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(documentsAfterDelete))
                .body("indices." + index + ".fields.string.doc_count", equalTo(documentsAfterDelete))
                .body("indices." + index + ".fields.string.sum_doc_freq", equalTo(documentsAfterDelete * 2));
    }

    @Test(timeout=60000)
    public void testBulkIngest() {
        final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
        final String index = "logs-" + UUID.randomUUID().toString();
        final String type = "test";

        TestUtils.disableMappingForIndex(index);

        given()
                .request()
                .body(generateBulkRequest(index, type, testDocument, totalDocuments))
                .when()
                .post("/_bulk")
                .then()
                .statusCode(200)
                .body("errors", equalTo(false))
                .body("items.size()", equalTo(totalDocuments));

        waitForBulkIngest(totalDocuments, index);

        ValidatableResponse response = getFieldStats(index, index,"string", false);
        while(response.extract().jsonPath().getInt("indices." + index + ".fields.string.max_doc") != totalDocuments) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {}
            response = getFieldStats(index, index, "string", false).log().all();
        }

        response.body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.doc_count", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.sum_doc_freq", equalTo(totalDocuments * 2));
    }

    @Test(timeout=60000)
    public void testBulkIngestPatternMatch() {
        final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
        final String index = "logs-" + UUID.randomUUID().toString();
        final String pattern = index.substring(0, index.length() - 2) + "*";
        final String type = "test";

        TestUtils.disableMappingForIndex(index);

        given()
                .request()
                .body(generateBulkRequest(index, type, testDocument, totalDocuments))
                .when()
                .post("/_bulk")
                .then()
                .statusCode(200)
                .body("errors", equalTo(false))
                .body("items.size()", equalTo(totalDocuments));

        waitForBulkIngest(totalDocuments, index);

        ValidatableResponse response = getFieldStats(pattern, index, "string", false)
                .log().all();
        while(response.extract().jsonPath().getInt("indices." + index + ".fields.string.max_doc") != totalDocuments) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {}
            response = getFieldStats(pattern, index, "string", false).log().all();
        }

        response.body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.doc_count", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.sum_doc_freq", equalTo(totalDocuments * 2));
    }

    private String generateBulkRequest(String index, String type, String document, int totalDocuments) {
        StringBuilder result = new StringBuilder();

        for(int i = 0; i < totalDocuments; i++) {
            result.append("{\"index\": { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" }}\n");
            result.append(document + "\n");
        }
        return result.toString();
    }

    private void waitForBulkIngest(int totalDocuments, String index) {
        final long startTime = System.currentTimeMillis();
        int result = 0;

        while(System.currentTimeMillis() - startTime < BULK_INDEX_TIMEOUT) {
            ValidatableResponse response = given()
                    .request()
                    .body("{\"query\":{\"match_all\":{}}, \"size\":" + (totalDocuments + 100) + "}")
                    .when()
                    .post("/" + index + "/_search")
                    .then()
                    .statusCode(200);
            result = response.extract().body().jsonPath().getInt("hits.total");
            if(result == totalDocuments) {
                return;
            }

            try {
                Thread.sleep(200);
            } catch (Exception e) {}
        }
        Assert.fail("Timed out while waiting for Bulk Ingest to finish. This is not an issue " +
                "with the realtime field stats but with the bulk API not ingesting the documents " +
                "in time. The respective test in the BulkApiTest class should be failing as well.");
    }

    private void submitDocumentNTimes(int n, String json, String index, String type) {
        for(int i = 0; i < n; i++) {
            given()
                .request()
                .body(json)
                .when()
                .post("/" + index + "/" + type)
                .then()
                .statusCode(201);
        }
    }

    private ValidatableResponse getFieldStats(String pattern, String index, String field, boolean clusterLevel) {
        ValidatableResponse validatableResponse = given()
                .request()
                .body("{\"fields\":[\"" + field + "\"]}")
                .when()
                .post("/" + pattern + "/_field_stats?level=" + (clusterLevel ? "cluster" : "indices"))
                .then()
                .statusCode(200);
        Map jsonData = validatableResponse.extract().body().jsonPath().getMap("indices." + index);
        while(jsonData == null || jsonData.isEmpty()) {
            try {
                Thread.sleep(100L);
            } catch (Exception e) {}
            validatableResponse = given()
                    .request()
                    .body("{\"fields\":[\"" + field + "\"]}")
                    .when()
                    .post("/" + pattern + "/_field_stats?level=" + (clusterLevel ? "cluster" : "indices"))
                    .then()
                    .statusCode(200);
            jsonData = validatableResponse.extract().body().jsonPath().getMap("indices." + index);
        }
        return validatableResponse;
    }
}
