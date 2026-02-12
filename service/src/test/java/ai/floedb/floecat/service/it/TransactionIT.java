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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.transaction.rpc.BeginTransactionRequest;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TransactionsGrpc;
import ai.floedb.floecat.transaction.rpc.TxChange;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class TransactionIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  TransactionsGrpc.TransactionsBlockingStub transactions;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void multiTableTransactionUpdatesTableName() {
    var cat = TestSupport.createCatalog(catalog, "tx-cat", "tx catalog");
    var parents = List.of("db_tx", "schema_tx");
    var nsLeaf = "it_ns_tx";
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), nsLeaf, parents, "ns for tx");

    String schemaJson = SchemaParser.toJson(SCHEMA);
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_tx",
            "s3://bucket/",
            schemaJson,
            "tx table");

    var begin = transactions.beginTransaction(BeginTransactionRequest.newBuilder().build());
    String txId = begin.getTransaction().getTxId();
    assertNotNull(txId);

    Table updated = tbl.toBuilder().setDisplayName("orders_tx_renamed").build();

    var change = TxChange.newBuilder().setTableId(tbl.getResourceId()).setTable(updated).build();

    var prepare =
        transactions.prepareTransaction(
            PrepareTransactionRequest.newBuilder().setTxId(txId).addChanges(change).build());
    assertEquals(TransactionState.TS_PREPARED, prepare.getTransaction().getState());
    var commit =
        transactions.commitTransaction(CommitTransactionRequest.newBuilder().setTxId(txId).build());
    assertEquals(TransactionState.TS_APPLIED, commit.getTransaction().getState());
    var fetchedTx =
        transactions.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
    assertEquals(TransactionState.TS_APPLIED, fetchedTx.getTransaction().getState());

    var fetched =
        table.getTable(GetTableRequest.newBuilder().setTableId(tbl.getResourceId()).build());
    assertEquals("orders_tx_renamed", fetched.getTable().getDisplayName());
  }

  @Test
  void multiTableTransactionAppliesAllTableRenamesAtomically() {
    var cat = TestSupport.createCatalog(catalog, "tx-cat-atomic", "tx catalog");
    var parents = List.of("db_tx", "schema_tx");
    var nsLeaf = "it_ns_tx_atomic";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for atomic tx");

    String schemaJson = SchemaParser.toJson(SCHEMA);
    var t1 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_atomic_a",
            "s3://bucket/",
            schemaJson,
            "tx table");
    var t2 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_atomic_b",
            "s3://bucket/",
            schemaJson,
            "tx table");

    var begin = transactions.beginTransaction(BeginTransactionRequest.newBuilder().build());
    String txId = begin.getTransaction().getTxId();
    assertNotNull(txId);

    Table t1Updated = t1.toBuilder().setDisplayName("orders_atomic_a_renamed").build();
    Table t2Updated = t2.toBuilder().setDisplayName("orders_atomic_b_renamed").build();

    var prepare =
        transactions.prepareTransaction(
            PrepareTransactionRequest.newBuilder()
                .setTxId(txId)
                .addChanges(
                    TxChange.newBuilder().setTableId(t1.getResourceId()).setTable(t1Updated))
                .addChanges(
                    TxChange.newBuilder().setTableId(t2.getResourceId()).setTable(t2Updated))
                .build());
    assertEquals(TransactionState.TS_PREPARED, prepare.getTransaction().getState());

    var commit =
        transactions.commitTransaction(CommitTransactionRequest.newBuilder().setTxId(txId).build());
    assertEquals(TransactionState.TS_APPLIED, commit.getTransaction().getState());

    var t1Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t1.getResourceId()).build())
            .getTable();
    var t2Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t2.getResourceId()).build())
            .getTable();
    assertEquals("orders_atomic_a_renamed", t1Fetched.getDisplayName());
    assertEquals("orders_atomic_b_renamed", t2Fetched.getDisplayName());
  }

  @Test
  void multiTableTransactionConflictDoesNotPartiallyApplyRenames() {
    var cat = TestSupport.createCatalog(catalog, "tx-cat-conflict", "tx catalog");
    var parents = List.of("db_tx", "schema_tx");
    var nsLeaf = "it_ns_tx_conflict";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for conflict tx");

    String schemaJson = SchemaParser.toJson(SCHEMA);
    var t1 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_conflict_a",
            "s3://bucket/",
            schemaJson,
            "tx table");
    var t2 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_conflict_b",
            "s3://bucket/",
            schemaJson,
            "tx table");

    var begin = transactions.beginTransaction(BeginTransactionRequest.newBuilder().build());
    String txId = begin.getTransaction().getTxId();
    assertNotNull(txId);

    String t1OriginalName = t1.getDisplayName();
    String t2OriginalName = t2.getDisplayName();
    String t1TxName = "orders_conflict_a_tx";
    String t2TxName = "orders_conflict_b_tx";
    Table t1Updated = t1.toBuilder().setDisplayName(t1TxName).build();
    Table t2Updated = t2.toBuilder().setDisplayName(t2TxName).build();

    var prepare =
        transactions.prepareTransaction(
            PrepareTransactionRequest.newBuilder()
                .setTxId(txId)
                .addChanges(
                    TxChange.newBuilder().setTableId(t1.getResourceId()).setTable(t1Updated))
                .addChanges(
                    TxChange.newBuilder().setTableId(t2.getResourceId()).setTable(t2Updated))
                .build());
    assertEquals(TransactionState.TS_PREPARED, prepare.getTransaction().getState());

    String t2ExternalName = "orders_conflict_b_external";
    TestSupport.renameTable(table, t2.getResourceId(), t2ExternalName);

    var commit =
        transactions.commitTransaction(CommitTransactionRequest.newBuilder().setTxId(txId).build());
    assertEquals(TransactionState.TS_APPLY_FAILED_CONFLICT, commit.getTransaction().getState());

    var t1Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t1.getResourceId()).build())
            .getTable();
    var t2Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t2.getResourceId()).build())
            .getTable();

    assertEquals(t1OriginalName, t1Fetched.getDisplayName());
    assertEquals(t2ExternalName, t2Fetched.getDisplayName());

    assertResolvesTo(
        "tx-cat-conflict",
        List.of("db_tx", "schema_tx", nsLeaf),
        t1OriginalName,
        t1.getResourceId());
    assertNotResolvable("tx-cat-conflict", List.of("db_tx", "schema_tx", nsLeaf), t1TxName);
    assertResolvesTo(
        "tx-cat-conflict",
        List.of("db_tx", "schema_tx", nsLeaf),
        t2ExternalName,
        t2.getResourceId());
    assertNotResolvable("tx-cat-conflict", List.of("db_tx", "schema_tx", nsLeaf), t2TxName);
  }

  @Test
  void multiTableTransactionNamePointerConflictIsAtomic() {
    var cat = TestSupport.createCatalog(catalog, "tx-cat-name-conflict", "tx catalog");
    var parents = List.of("db_tx", "schema_tx");
    var nsLeaf = "it_ns_tx_name_conflict";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for name conflict tx");

    String schemaJson = SchemaParser.toJson(SCHEMA);
    var t1 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_name_conflict_a",
            "s3://bucket/",
            schemaJson,
            "tx table");
    var t2 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_name_conflict_b",
            "s3://bucket/",
            schemaJson,
            "tx table");
    var t3 =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_name_conflict_c",
            "s3://bucket/",
            schemaJson,
            "tx table");

    var begin = transactions.beginTransaction(BeginTransactionRequest.newBuilder().build());
    String txId = begin.getTransaction().getTxId();
    assertNotNull(txId);

    String t1OriginalName = t1.getDisplayName();
    String t2OriginalName = t2.getDisplayName();
    String t3OriginalName = t3.getDisplayName();
    String contestedName = "orders_name_conflict_contested";
    String t2TxName = "orders_name_conflict_b_tx";
    Table t1Updated = t1.toBuilder().setDisplayName(contestedName).build();
    Table t2Updated = t2.toBuilder().setDisplayName(t2TxName).build();

    var prepare =
        transactions.prepareTransaction(
            PrepareTransactionRequest.newBuilder()
                .setTxId(txId)
                .addChanges(
                    TxChange.newBuilder().setTableId(t1.getResourceId()).setTable(t1Updated))
                .addChanges(
                    TxChange.newBuilder().setTableId(t2.getResourceId()).setTable(t2Updated))
                .build());
    assertEquals(TransactionState.TS_PREPARED, prepare.getTransaction().getState());

    // Claim the contested name after prepare and before commit.
    TestSupport.renameTable(table, t3.getResourceId(), contestedName);

    var commit =
        transactions.commitTransaction(CommitTransactionRequest.newBuilder().setTxId(txId).build());
    assertEquals(TransactionState.TS_APPLY_FAILED_CONFLICT, commit.getTransaction().getState());

    var t1Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t1.getResourceId()).build())
            .getTable();
    var t2Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t2.getResourceId()).build())
            .getTable();
    var t3Fetched =
        table
            .getTable(GetTableRequest.newBuilder().setTableId(t3.getResourceId()).build())
            .getTable();

    assertEquals(t1OriginalName, t1Fetched.getDisplayName());
    assertEquals(t2OriginalName, t2Fetched.getDisplayName());
    assertEquals(contestedName, t3Fetched.getDisplayName());

    assertResolvesTo(
        "tx-cat-name-conflict",
        List.of("db_tx", "schema_tx", nsLeaf),
        t1OriginalName,
        t1.getResourceId());
    assertResolvesTo(
        "tx-cat-name-conflict",
        List.of("db_tx", "schema_tx", nsLeaf),
        t2OriginalName,
        t2.getResourceId());
    assertResolvesTo(
        "tx-cat-name-conflict",
        List.of("db_tx", "schema_tx", nsLeaf),
        contestedName,
        t3.getResourceId());
    assertNotResolvable("tx-cat-name-conflict", List.of("db_tx", "schema_tx", nsLeaf), t2TxName);
    assertNotResolvable(
        "tx-cat-name-conflict", List.of("db_tx", "schema_tx", nsLeaf), t3OriginalName);
  }

  private void assertResolvesTo(
      String catalogName,
      List<String> namespacePath,
      String tableName,
      ai.floedb.floecat.common.rpc.ResourceId expected) {
    var resolved = TestSupport.resolveTableId(directory, catalogName, namespacePath, tableName);
    assertEquals(expected.getId(), resolved.getId());
  }

  private void assertNotResolvable(
      String catalogName, List<String> namespacePath, String tableName) {
    try {
      TestSupport.resolveTableId(directory, catalogName, namespacePath, tableName);
      throw new AssertionError("expected resolution failure for " + tableName);
    } catch (StatusRuntimeException e) {
      assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }
  }
}
