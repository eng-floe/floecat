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
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests schema resolution for specific snapshots using DescribeInputs().
 *
 * <p>BeginQuery no longer returns schema; DescribeInputs now does.
 */
@QuarkusTest
class QueryServiceSchemaIT {

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaSvc;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub stats;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void describeInputsReturnsSchemaPerSnapshot() {

    // ------------------------------
    // Create catalog, namespace, table
    // ------------------------------
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "cat1", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");

    Schema schemaV1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    Schema schemaV2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "qty", Types.IntegerType.get()));

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(schemaV1),
            "desc");

    // snapshot 1 → schemaV1
    var snap1 =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);

    // update table schema → schemaV2
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSpec(
                TableSpec.newBuilder()
                    .setSchemaJson(SchemaParser.toJson(schemaV2))
                    .setUpstream(tbl.getUpstream()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());

    // snapshot 2 → schemaV2
    var snap2 =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 2L, System.currentTimeMillis() - 5_000L);

    // Fully qualified name
    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    // A table pin is per-query and first-touch: one query cannot pin the same table at two
    // different snapshots (that is a conflicting temporal intent). Each snapshot is therefore
    // resolved in its own query, which still proves schema resolves per pinned snapshot.

    // ------------------------------
    // Query A pins snapshot1 → schemaV1
    // ------------------------------
    var beginA =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(10)
                .build());
    String qidA = beginA.getQuery().getQueryId();
    assertFalse(qidA.isBlank(), "BeginQuery must return queryId");

    var resp1 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qidA)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId())))
                .build());

    assertEquals(1, resp1.getSchemasCount());
    assertEquals(1, resp1.getSchemas(0).getColumnsCount(), "snapshot1 schema should have 1 column");
    // The response exposes one positional pin identity per input, resolved to the pinned snapshot.
    assertEquals(1, resp1.getRelationPinsCount());
    assertEquals(snap1.getSnapshotId(), resp1.getRelationPins(0).getSnapshotId());
    assertFalse(resp1.getRelationPins(0).getPinFingerprint().isEmpty());

    // ------------------------------
    // Query B pins snapshot2 → schemaV2
    // ------------------------------
    var beginB =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(10)
                .build());
    String qidB = beginB.getQuery().getQueryId();

    var resp2 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qidB)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap2.getSnapshotId())))
                .build());

    assertEquals(1, resp2.getSchemasCount());
    assertEquals(
        2,
        resp2.getSchemas(0).getColumnsCount(),
        "snapshot2 schema should expose v2 with 2 columns");

    assertTrue(
        resp2.getSchemas(0).getColumnsList().stream().anyMatch(c -> c.getName().equals("qty")),
        "schemaV2 must include column 'qty'");
    assertEquals(1, resp2.getRelationPinsCount());
    assertEquals(snap2.getSnapshotId(), resp2.getRelationPins(0).getSnapshotId());
    // Different pinned snapshot ⇒ different opaque identity than query A's pin.
    assertNotEquals(
        resp1.getRelationPins(0).getPinFingerprint(), resp2.getRelationPins(0).getPinFingerprint());
  }

  @Test
  void describeInputsHandlesViewInputs() {
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "cat_view", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");
    var createdView =
        TestSupport.createView(
            view, cat.getResourceId(), ns.getResourceId(), "orders_view", "select 1", "desc");

    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName(createdView.getDisplayName())
            .build();

    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(10)
                .build());
    String qid = begin.getQuery().getQueryId();

    var response =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(QueryInput.newBuilder().setName(name))
                .build());

    assertEquals(1, response.getSchemasCount());
    assertEquals(
        1,
        response.getSchemas(0).getColumnsCount(),
        "view schema should contain the output columns stored at creation time");
  }

  @Test
  void describeInputsReturnsSchemasInRequestOrder() {
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "table_view", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_multi",
            "s3://bucket/orders_multi",
            SchemaParser.toJson(schema),
            "desc");
    var snap =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 1L, System.currentTimeMillis() - 5_000L);

    var viewNode =
        TestSupport.createView(
            view,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders_view_multi",
            "select 1 as view_id",
            "desc");

    var tableName =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName(tbl.getDisplayName())
            .build();
    var viewName =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName(viewNode.getDisplayName())
            .build();

    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(10)
                .build());
    String qid = begin.getQuery().getQueryId();

    var resp =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(tableName)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId())))
                .addInputs(QueryInput.newBuilder().setName(viewName))
                .build());

    assertEquals(2, resp.getSchemasCount());
    assertTrue(resp.getSchemas(0).getColumnsCount() > 0, "first schema should correspond to table");
    assertEquals(
        1,
        resp.getSchemas(1).getColumnsCount(),
        "second schema is the view with its stored output columns");

    // Pin identities are positional with schemas: the table input carries its pin identity, the
    // view input carries the empty default (views pin their base tables, not themselves).
    assertEquals(2, resp.getRelationPinsCount());
    assertEquals(snap.getSnapshotId(), resp.getRelationPins(0).getSnapshotId());
    assertFalse(resp.getRelationPins(0).getPinFingerprint().isEmpty());
    assertTrue(
        resp.getRelationPins(1).getPinFingerprint().isEmpty(),
        "view position carries the empty default pin identity");
  }

  @Test
  void describeInputsDescribesTheWinningPinWhenReReferencedAsCurrentInSameQuery() {
    // Regression for the winner-vs-candidate fix: a table first-touched at an explicit snapshot,
    // then re-referenced as CURRENT in the SAME query, must keep describing the first (winning) pin
    // even though CURRENT would resolve to the newer snapshot. This is the exact divergence the
    // separate-query test cannot exercise (there candidate == winner).
    var cat = TestSupport.createCatalog(catalog, catalogPrefix + "winner", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "desc");

    Schema schemaV1 = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema schemaV2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "qty", Types.IntegerType.get()));

    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            SchemaParser.toJson(schemaV1),
            "desc");
    var snap1 =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tbl.getResourceId())
            .setSpec(
                TableSpec.newBuilder()
                    .setSchemaJson(SchemaParser.toJson(schemaV2))
                    .setUpstream(tbl.getUpstream()))
            .setUpdateMask(FieldMask.newBuilder().addPaths("schema_json").build())
            .build());
    var snap2 =
        TestSupport.createFinalizedSnapshot(
            snapshot, stats, tbl.getResourceId(), 2L, System.currentTimeMillis() - 5_000L);

    var name =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .setDefaultCatalogId(cat.getResourceId())
                .setTtlSeconds(30)
                .build());
    String qid = begin.getQuery().getQueryId();

    // First touch: pin explicitly at snapshot1 (schemaV1).
    var resp1 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId())))
                .build());
    assertEquals(1, resp1.getSchemas(0).getColumnsCount());

    // Re-reference the same table with no snapshot (CURRENT → would resolve to snapshot2/schemaV2),
    // in the same query. First-touch wins, so it must still describe snapshot1/schemaV1.
    var resp2 =
        schemaSvc.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(qid)
                .addInputs(QueryInput.newBuilder().setName(name))
                .build());

    assertEquals(
        1,
        resp2.getSchemas(0).getColumnsCount(),
        "CURRENT re-reference must describe the winning snapshot1 pin, not current snapshot2");
    assertEquals(snap1.getSnapshotId(), resp2.getRelationPins(0).getSnapshotId());
    assertNotEquals(snap2.getSnapshotId(), resp2.getRelationPins(0).getSnapshotId());
  }
}
