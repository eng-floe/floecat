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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TransactionClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitMaterializationService;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class TableCommitResourceTest extends AbstractRestResourceTest {

  @InjectMock TransactionClient transactionClient;
  @InjectMock TableCommitMaterializationService materializationService;

  @BeforeEach
  void setUpAtomicCommitDefaults() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                MaterializeMetadataResult.success(
                    invocation.getArgument(4, TableMetadataView.class),
                    invocation.getArgument(5, String.class)));
  }

  @Test
  void commitTransactionRequiresTableChanges() {
    given()
        .body("{\"table-changes\":[]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/transactions/commit")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"))
        .body("error.message", containsString("tableChanges size must be between 1"));
  }

  @Test
  void commitSupportsSetLocationUpdateViaAtomicTransaction() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .setUri("s3://bucket/path/")
            .build();
    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .putProperties("location", "s3://bucket/path/")
            .putProperties("metadata-location", "s3://bucket/path/metadata/00001.metadata.json")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body(
            "{\"requirements\":[],\"updates\":[{\"action\":\"set-location\",\"location\":\"s3://bucket/new_path/\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<PrepareTransactionRequest> requestCaptor =
        ArgumentCaptor.forClass(PrepareTransactionRequest.class);
    verify(transactionClient).prepareTransaction(requestCaptor.capture());
    String committedUri =
        requestCaptor.getValue().getChangesList().stream()
            .filter(change -> change.hasTable() && change.getTable().hasUpstream())
            .map(change -> change.getTable().getUpstream().getUri())
            .filter(uri -> uri != null && !uri.isBlank())
            .findFirst()
            .orElse("");
    assertEquals("s3://bucket/new_path/", committedUri);
  }

  @Test
  void commitAddSnapshotDoesNotDirectlyMutateSnapshotsOutsideTransaction() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties(
                "metadata-location", "s3://warehouse/db/orders/metadata/00001.metadata.json")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());

    given()
        .body(
            """
            {"requirements":[],"updates":[{"action":"add-snapshot","snapshot":{
              "snapshot-id":7,
              "timestamp-ms":1000,
              "manifest-list":"s3://warehouse/db/orders/metadata/manifest.avro",
              "summary":{"operation":"append"}
            }}]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    verify(transactionClient).commitTransaction(any());
  }

  @Test
  void commitRequirementTableUuidMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("table-uuid", "actual-uuid")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body(
            "{\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"expected\"}],\"updates\":[]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void commitRequirementAssertRefSnapshotIdMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("current-snapshot-id", "101")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body(
            """
            {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":999}],"updates":[]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(transactionClient, never()).commitTransaction(any());
    verify(transactionClient).abortTransaction(any());
  }
}
