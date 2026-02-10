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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.transaction.rpc.BeginTransactionRequest;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionsGrpc;
import ai.floedb.floecat.transaction.rpc.TxChange;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
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

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);

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

    transactions.prepareTransaction(
        PrepareTransactionRequest.newBuilder().setTxId(txId).addChanges(change).build());
    transactions.commitTransaction(CommitTransactionRequest.newBuilder().setTxId(txId).build());

    var fetched =
        table.getTable(GetTableRequest.newBuilder().setTableId(tbl.getResourceId()).build());
    assertEquals("orders_tx_renamed", fetched.getTable().getDisplayName());

    directory.resolveTable(
        ai.floedb.floecat.catalog.rpc.ResolveTableRequest.newBuilder()
            .setRef(
                NameRef.newBuilder()
                    .setCatalog(cat.getDisplayName())
                    .addAllPath(nsPath)
                    .setName("orders_tx_renamed"))
            .build());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            directory.resolveTable(
                ai.floedb.floecat.catalog.rpc.ResolveTableRequest.newBuilder()
                    .setRef(
                        NameRef.newBuilder()
                            .setCatalog(cat.getDisplayName())
                            .addAllPath(nsPath)
                            .setName("orders_tx"))
                    .build()));
  }
}
