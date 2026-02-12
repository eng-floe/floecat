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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TransactionClient;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class TransactionCommitResourceTest extends AbstractRestResourceTest {

  @InjectMock TransactionClient transactionClient;

  @BeforeEach
  void setUpTableLookup() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("foo:db:orders"))
            .putProperties("location", "s3://warehouse/db/orders")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());
  }

  @Test
  void transactionCommitReturnsNoContentOnlyWhenApplied() {
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

    given()
        .body(requestBody())
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/transactions/commit")
        .then()
        .statusCode(204);
  }

  @Test
  void transactionCommitReturnsStateUnknownWhenBackendApplyIsRetryable() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-2"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-2").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-2").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-2")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());

    given()
        .body(requestBody())
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/transactions/commit")
        .then()
        .statusCode(503)
        .body("error.type", org.hamcrest.Matchers.equalTo("CommitStateUnknownException"));
  }

  @Test
  void transactionCommitReturnsConflictWhenBackendApplyConflicts() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-3"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-3").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-3").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-3")
                        .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
                .build());

    given()
        .body(requestBody())
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/transactions/commit")
        .then()
        .statusCode(409)
        .body("error.type", org.hamcrest.Matchers.equalTo("CommitFailedException"));
  }

  @Test
  void transactionCommitMissingAuthHeaderReturns401() {
    RestAssured.requestSpecification = null;
    given()
        .body(requestBody())
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/transactions/commit")
        .then()
        .statusCode(401)
        .body("error.code", org.hamcrest.Matchers.equalTo(401));
    RestAssured.requestSpecification = defaultSpec;
  }

  private Map<String, Object> requestBody() {
    return Map.of(
        "table-changes",
        List.of(
            Map.of(
                "identifier",
                Map.of("namespace", List.of("db"), "name", "orders"),
                "requirements",
                List.of(),
                "updates",
                List.of())));
  }
}
