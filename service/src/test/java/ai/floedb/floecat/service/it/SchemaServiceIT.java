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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.SnapshotRef;
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

@QuarkusTest
class SchemaServiceIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  SchemaServiceGrpc.SchemaServiceBlockingStub schemas;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  String prefix = this.getClass().getSimpleName() + "_";

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void schemaReturnsSnapshotVersion() {
    var cat = TestSupport.createCatalog(catalog, prefix + "cat", "");
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
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 1L, System.currentTimeMillis() - 10_000L);

    // Update schema to v2 and take another snapshot
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
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 2L, System.currentTimeMillis() - 5_000L);

    // Current schema should be v2
    var currentSchema =
        schemas
            .getSchema(GetSchemaRequest.newBuilder().setTableId(tbl.getResourceId()).build())
            .getSchema();
    assertEquals(2, currentSchema.getColumnsCount(), "current schema should reflect latest table");

    // Snapshot 1 should return v1 schema
    var snap1Schema =
        schemas
            .getSchema(
                GetSchemaRequest.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap1.getSnapshotId()))
                    .build())
            .getSchema();
    assertEquals(1, snap1Schema.getColumnsCount(), "snapshot 1 should have original schema");
    assertEquals("id", snap1Schema.getColumns(0).getName());

    // Snapshot 2 should return v2 schema
    var snap2Schema =
        schemas
            .getSchema(
                GetSchemaRequest.newBuilder()
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap2.getSnapshotId()))
                    .build())
            .getSchema();
    assertEquals(2, snap2Schema.getColumnsCount(), "snapshot 2 should have updated schema");
    assertTrue(
        snap2Schema.getColumnsList().stream().anyMatch(c -> c.getName().equals("qty")),
        "updated column should be present");
  }
}
