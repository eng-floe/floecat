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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.*;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the QueryService lifecycle (begin/renew/end/get).
 *
 * <p>After API changes: - BeginQuery no longer accepts inputs - DescribeInputs is now required
 * before scanning or planning
 */
@QuarkusTest
class QueryServiceIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  /** Verifies BeginQuery, DescribeInputs, RenewQuery, and EndQuery lifecycle behavior. */
  @Test
  void queryBeginRenewEnd() {
    var catName = catalogPrefix + "cat1";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var snap =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 0L, System.currentTimeMillis() - 10_000L);

    var name =
        NameRef.newBuilder()
            .setCatalog(catName)
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    //
    // === NEW API: BeginQuery takes no inputs ===
    //
    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(2)
                .build());

    assertTrue(begin.hasQuery());
    var query = begin.getQuery();
    assertFalse(query.getQueryId().isBlank(), "BeginQuery must return a queryId");

    //
    // === NEW API: DescribeInputs pins tables + snapshots ===
    //
    var desc =
        schema.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(query.getQueryId())
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId())))
                .build());

    // Should return exactly one schema
    assertEquals(1, desc.getSchemasCount(), "DescribeInputs must return matching schemas");

    //
    // === RenewQuery ===
    //
    var renew =
        queries.renewQuery(
            RenewQueryRequest.newBuilder().setQueryId(query.getQueryId()).setTtlSeconds(2).build());

    assertEquals(query.getQueryId(), renew.getQueryId());

    //
    // === EndQuery ===
    //
    var end =
        queries.endQuery(
            EndQueryRequest.newBuilder().setQueryId(query.getQueryId()).setCommit(true).build());

    assertEquals(query.getQueryId(), end.getQueryId());
  }
}
