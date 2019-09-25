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

import java.util.Random;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { ElefanaApplication.class })
@TestPropertySource(locations = "classpath:es2.properties")
public class RealtimeFieldStatsServiceTest {
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

    @Test
    public void testFieldStatsGeneration() {
        final String index = UUID.randomUUID().toString();
        final String type = "test";

        final int totalDocuments = 100;
        final int totalEmptyFieldDocuments = 40;

        submitDocumentNTimes(totalEmptyFieldDocuments, testDocumentNoBool, index, type);
        submitDocumentNTimes(totalDocuments - totalEmptyFieldDocuments, testDocument, index, type);

        getFieldStats(index, "string", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.string.doc_count", equalTo(totalDocuments));

        getFieldStats(index, "bool", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.bool.max_doc", equalTo(totalDocuments))
                .body("indices." + index + ".fields.bool.doc_count", equalTo(totalDocuments - totalEmptyFieldDocuments))
                .body("indices." + index + ".fields.bool.max_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.max_value_as_string", isA(String.class))
                .body("indices." + index + ".fields.bool.min_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.min_value_as_string", isA(String.class));
    }

    @Test
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

        getFieldStats(index, "string", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(documentsAfterDelete))
                .body("indices." + index + ".fields.string.doc_count", equalTo(documentsAfterDelete))
                .body("indices." + index + ".fields.string.sum_doc_freq", equalTo(documentsAfterDelete * 2));
    }

    @Test
    public void testBulkIngest() {
        final int totalDocuments = PsqlBulkIngestService.MINIMUM_BULK_SIZE + RANDOM.nextInt(PsqlBulkIngestService.MINIMUM_BULK_SIZE);
        final String index = "logs-" + UUID.randomUUID().toString();
        final String type = "test";

        TestUtils.disableMappingAndStatsForIndex(index);

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

        getFieldStats(index, "string", false)
                .log().all()
                .body("_shards.successful", equalTo(1))
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

    @Test
    public void testDeleteDocument() {
        final String index = UUID.randomUUID().toString();
        final String type = "test";

        final int totalEmptyFieldDocuments = 40;
        final int totalAllFieldDocuments = 60;
        final int totalDeletedEmptyFieldDocuments = 10;
        final int totalDeletedAllFieldDocuments = 20;
        final int totalDocuments = totalAllFieldDocuments + totalEmptyFieldDocuments;
        final int totalDeletedDocuments = totalDeletedAllFieldDocuments + totalDeletedEmptyFieldDocuments;

        submitDocumentNTimes(totalEmptyFieldDocuments, 1, testDocumentNoBool, index, type);
        submitDocumentNTimes(totalAllFieldDocuments, totalEmptyFieldDocuments + 1, testDocument, index, type);

        deleteDocumentNTimes(totalDeletedEmptyFieldDocuments, 1, index, type);
        deleteDocumentNTimes(totalDeletedAllFieldDocuments, totalEmptyFieldDocuments + 1, index, type);

        getFieldStats(index, "string", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.string.max_doc", equalTo(totalDocuments - totalDeletedDocuments))
                .body("indices." + index + ".fields.string.doc_count", equalTo(totalDocuments - totalDeletedDocuments));

        getFieldStats(index, "bool", false)
                .body("_shards.successful", equalTo(1))
                .body("indices." + index + ".fields.bool.max_doc", equalTo(totalDocuments - totalDeletedDocuments))
                .body("indices." + index + ".fields.bool.doc_count", equalTo(totalAllFieldDocuments - totalDeletedAllFieldDocuments))
                .body("indices." + index + ".fields.bool.max_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.max_value_as_string", isA(String.class))
                .body("indices." + index + ".fields.bool.min_value", isA(Boolean.class))
                .body("indices." + index + ".fields.bool.min_value_as_string", isA(String.class));
    }

    private void submitDocumentNTimes(int n, String json, String index, String type) {
        submitDocumentNTimes(n, 1, json, index, type);
    }

    private void submitDocumentNTimes(int n, int startId, String json, String index, String type) {
        for(int i = startId; i < startId + n; i++) {
            given()
                    .request()
                    .body(json)
                    .when()
                    .post("/" + index + "/" + type + "/" + i)
                    .then()
                    .statusCode(201);
        }
    }

    private void deleteDocumentNTimes(int n, int startId, String index, String type){
        for(int i = startId; i < startId + n; i++) {
            given()
                    .when()
                    .delete("/" + index + "/" + type + "/" + i)
                    .then()
                    .statusCode(200);
        }
    }

    private ValidatableResponse getFieldStats(String index, String field, boolean clusterLevel) {
        return given()
                .request()
                .body("{\"fields\":[\"" + field + "\"]}")
                .when()
                .post("/" + index + "/_field_stats?level=" + clusterLevel)
                .then()
                .statusCode(200);
    }
}
