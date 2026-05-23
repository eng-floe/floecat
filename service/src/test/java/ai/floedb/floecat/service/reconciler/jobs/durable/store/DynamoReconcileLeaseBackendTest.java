/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

class DynamoReconcileLeaseBackendTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String JOB_ID = "job-1";

  private Config config;
  private DynamoDbClient dynamoDbClient;
  private DynamoReconcileLeaseBackend backend;

  @BeforeEach
  void setUp() {
    config = ConfigProvider.getConfig();
    Assumptions.assumeTrue(isDynamoMode(), "localstack-only Dynamo lease backend assertions");
    dynamoDbClient = createDynamoDbClient();
    clearDynamoTable();
    backend = new DynamoReconcileLeaseBackend();
    backend.bind(dynamoDbClient, kvTable());
  }

  @AfterEach
  void tearDown() {
    if (dynamoDbClient != null) {
      dynamoDbClient.close();
      dynamoDbClient = null;
    }
  }

  @Test
  void compareAndSetBatchAllowsConditionAndMutationOnSameLeaseRow() {
    assertTrue(
        backend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseRecordUpsert(
                        ACCOUNT_ID, JOB_ID, 0L, "encoded-lease-v1")))));

    assertTrue(
        backend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseRecordCondition(ACCOUNT_ID, JOB_ID, 1L),
                    new ReconcileLeaseBackend.LeaseRecordUpsert(
                        ACCOUNT_ID, JOB_ID, 1L, "encoded-lease-v2")))));

    var updated = backend.loadLease(ACCOUNT_ID, JOB_ID).orElseThrow();
    assertEquals("encoded-lease-v2", updated.encodedLease());
    assertEquals(2L, updated.version());

    assertTrue(
        backend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseRecordCondition(ACCOUNT_ID, JOB_ID, 2L),
                    new ReconcileLeaseBackend.LeaseRecordDelete(ACCOUNT_ID, JOB_ID, 2L)))));

    assertTrue(backend.loadLease(ACCOUNT_ID, JOB_ID).isEmpty());
  }

  private boolean isDynamoMode() {
    return "dynamodb"
        .equalsIgnoreCase(config.getOptionalValue("floecat.kv", String.class).orElse("memory"));
  }

  private String kvTable() {
    return config.getOptionalValue("floecat.kv.table", String.class).orElse("floecat_pointers");
  }

  private DynamoDbClient createDynamoDbClient() {
    String endpoint =
        config
            .getOptionalValue("floecat.storage.aws.dynamodb.endpoint-override", String.class)
            .orElse("http://localhost:4566");
    String region =
        config.getOptionalValue("floecat.storage.aws.region", String.class).orElse("us-east-1");
    String accessKey =
        config.getOptionalValue("floecat.storage.aws.access-key-id", String.class).orElse("test");
    String secretKey =
        config
            .getOptionalValue("floecat.storage.aws.secret-access-key", String.class)
            .orElse("test");
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .build();
  }

  private void clearDynamoTable() {
    Map<String, AttributeValue> startKey = null;
    do {
      var request = ScanRequest.builder().tableName(kvTable());
      if (startKey != null && !startKey.isEmpty()) {
        request.exclusiveStartKey(startKey);
      }
      var response = dynamoDbClient.scan(request.build());
      for (var item : response.items()) {
        dynamoDbClient.deleteItem(
            DeleteItemRequest.builder()
                .tableName(kvTable())
                .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                .build());
      }
      startKey = response.lastEvaluatedKey();
    } while (startKey != null && !startKey.isEmpty());
  }
}
