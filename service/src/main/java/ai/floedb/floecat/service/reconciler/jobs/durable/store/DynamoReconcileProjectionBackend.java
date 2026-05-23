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

import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_KIND;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_PARTITION_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_SORT_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_VERSION;

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileProjectionBackend implements ReconcileProjectionBackend {
  @Inject DynamoDbClient dynamoDb;

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  @Inject
  public DynamoReconcileProjectionBackend() {}

  public void bind(DynamoDbClient dynamoDb, String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  @Override
  public Optional<ContributionSnapshot> loadContribution(
      String accountId, String parentJobId, String childJobId) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            ProjectionBackendSupport.contributionPartitionKey(
                                accountId, parentJobId)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(
                            ProjectionBackendSupport.contributionSortKey(childJobId))))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new ContributionSnapshot(
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_ACCOUNT_ID),
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_PARENT_JOB_ID),
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_CHILD_JOB_ID),
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_BLOB_URI),
            longAttr(response.item(), ATTR_VERSION)));
  }

  @Override
  public List<ContributionSnapshot> listContributions(String accountId, String parentJobId) {
    var response =
        dynamoDb.query(
            QueryRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                .keyConditionExpression("#pk = :pk")
                .expressionAttributeValues(
                    Map.of(
                        ":pk",
                        AttributeValue.fromS(
                            ProjectionBackendSupport.contributionPartitionKey(
                                accountId, parentJobId))))
                .build());
    List<ContributionSnapshot> out = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      out.add(
          new ContributionSnapshot(
              stringAttr(item, ProjectionBackendSupport.ATTR_ACCOUNT_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_PARENT_JOB_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_CHILD_JOB_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_BLOB_URI),
              longAttr(item, ATTR_VERSION)));
    }
    return out;
  }

  @Override
  public List<ContributionSnapshot> listContributionsForChild(String accountId, String childJobId) {
    var response =
        dynamoDb.scan(
            ScanRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .expressionAttributeNames(
                    Map.of(
                        "#kind", ATTR_KIND,
                        "#account", ProjectionBackendSupport.ATTR_ACCOUNT_ID,
                        "#child", ProjectionBackendSupport.ATTR_CHILD_JOB_ID))
                .filterExpression("#kind = :kind and #account = :account and #child = :child")
                .expressionAttributeValues(
                    Map.of(
                        ":kind",
                        AttributeValue.fromS(ProjectionBackendSupport.KIND_CONTRIBUTION),
                        ":account",
                        AttributeValue.fromS(accountId),
                        ":child",
                        AttributeValue.fromS(childJobId)))
                .build());
    List<ContributionSnapshot> out = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      out.add(
          new ContributionSnapshot(
              stringAttr(item, ProjectionBackendSupport.ATTR_ACCOUNT_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_PARENT_JOB_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_CHILD_JOB_ID),
              stringAttr(item, ProjectionBackendSupport.ATTR_BLOB_URI),
              longAttr(item, ATTR_VERSION)));
    }
    return out;
  }

  @Override
  public Optional<ResultReferenceSnapshot> loadResultReference(String accountId, String jobId) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            ProjectionBackendSupport.resultReferencePartitionKey(accountId)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(
                            ProjectionBackendSupport.resultReferenceSortKey(jobId))))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new ResultReferenceSnapshot(
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_ACCOUNT_ID),
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_JOB_ID),
            stringAttr(response.item(), ProjectionBackendSupport.ATTR_BLOB_URI),
            longAttr(response.item(), ATTR_VERSION)));
  }

  @Override
  public boolean deleteContribution(String accountId, String parentJobId, String childJobId) {
    dynamoDb.deleteItem(
        DeleteItemRequest.builder()
            .tableName(table)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY,
                    AttributeValue.fromS(
                        ProjectionBackendSupport.contributionPartitionKey(accountId, parentJobId)),
                    ATTR_SORT_KEY,
                    AttributeValue.fromS(ProjectionBackendSupport.contributionSortKey(childJobId))))
            .build());
    return true;
  }

  @Override
  public boolean deleteResultReference(String accountId, String jobId) {
    dynamoDb.deleteItem(
        DeleteItemRequest.builder()
            .tableName(table)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY,
                    AttributeValue.fromS(
                        ProjectionBackendSupport.resultReferencePartitionKey(accountId)),
                    ATTR_SORT_KEY,
                    AttributeValue.fromS(ProjectionBackendSupport.resultReferenceSortKey(jobId))))
            .build());
    return true;
  }

  @Override
  public boolean compareAndSetBatch(ProjectionWriteBatch batch) {
    if (batch == null || batch.writes().isEmpty()) {
      return true;
    }
    List<TransactWriteItem> tx = new ArrayList<>(batch.writes().size());
    for (ProjectionWriteOp write : batch.writes()) {
      if (write instanceof ContributionUpsert upsert) {
        tx.add(buildContributionUpsert(upsert));
      } else if (write instanceof ResultReferenceUpsert upsert) {
        tx.add(buildResultReferenceUpsert(upsert));
      }
    }
    try {
      dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(tx).build());
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  private TransactWriteItem buildContributionUpsert(ContributionUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(
            ProjectionBackendSupport.contributionPartitionKey(
                upsert.accountId(), upsert.parentJobId())));
    item.put(
        ATTR_SORT_KEY,
        AttributeValue.fromS(ProjectionBackendSupport.contributionSortKey(upsert.childJobId())));
    item.put(ATTR_KIND, AttributeValue.fromS(ProjectionBackendSupport.KIND_CONTRIBUTION));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(ProjectionBackendSupport.ATTR_ACCOUNT_ID, AttributeValue.fromS(upsert.accountId()));
    item.put(
        ProjectionBackendSupport.ATTR_PARENT_JOB_ID, AttributeValue.fromS(upsert.parentJobId()));
    item.put(ProjectionBackendSupport.ATTR_CHILD_JOB_ID, AttributeValue.fromS(upsert.childJobId()));
    item.put(ProjectionBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(upsert.blobUri()));
    return TransactWriteItem.builder().put(conditionalPut(item, upsert.expectedVersion())).build();
  }

  private TransactWriteItem buildResultReferenceUpsert(ResultReferenceUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(
            ProjectionBackendSupport.resultReferencePartitionKey(upsert.accountId())));
    item.put(
        ATTR_SORT_KEY,
        AttributeValue.fromS(ProjectionBackendSupport.resultReferenceSortKey(upsert.jobId())));
    item.put(ATTR_KIND, AttributeValue.fromS(ProjectionBackendSupport.KIND_RESULT_REFERENCE));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(ProjectionBackendSupport.ATTR_ACCOUNT_ID, AttributeValue.fromS(upsert.accountId()));
    item.put(ProjectionBackendSupport.ATTR_JOB_ID, AttributeValue.fromS(upsert.jobId()));
    item.put(ProjectionBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(upsert.blobUri()));
    return TransactWriteItem.builder().put(conditionalPut(item, upsert.expectedVersion())).build();
  }

  private Put conditionalPut(Map<String, AttributeValue> item, long expectedVersion) {
    Put.Builder put = Put.builder().tableName(table).item(item);
    if (expectedVersion == 0L) {
      put.conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      put.conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))));
    }
    return put.build();
  }

  private static String stringAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value == null || value.s() == null ? "" : value.s();
  }

  private static long longAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    if (value == null || value.n() == null) {
      return 0L;
    }
    try {
      return Long.parseLong(value.n());
    } catch (NumberFormatException ignored) {
      return 0L;
    }
  }
}
