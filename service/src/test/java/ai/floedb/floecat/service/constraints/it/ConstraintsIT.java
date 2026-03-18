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

package ai.floedb.floecat.service.constraints.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ConstraintsIT {

  @Inject ConstraintRepository constraintRepository;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistic;

  @GrpcClient("floecat")
  TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService;

  String tablePrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void putTableConstraintsStoresConstraintsForSnapshot() throws Exception {
    var catName = tablePrefix + "cat_constraints";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints",
            "s3://bucket/fact_constraints",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tblId = tbl.getResourceId();
    long snapshotId = 303L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tblId.toBuilder().setId("wrong-id-should-be-overwritten").build())
            .setSnapshotId(snapshotId + 1)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_fact_constraints")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tblId, snapshotId).orElseThrow();
    assertEquals(tblId, stored.getTableId());
    assertEquals(snapshotId, stored.getSnapshotId());
    assertEquals(1, stored.getConstraintsCount());
    assertEquals("pk_fact_constraints", stored.getConstraints(0).getName());
  }

  @Test
  void putTableConstraintsRejectsMissingConstraintsPayload() throws Exception {
    var catName = tablePrefix + "cat_constraints_missing_payload";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints missing payload");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_missing_payload", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_missing_payload",
            "s3://bucket/fact_constraints_missing_payload",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tblId = tbl.getResourceId();
    long snapshotId = 302L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshotId(snapshotId)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "constraints");
    assertTrue(constraintRepository.getSnapshotConstraints(tblId, snapshotId).isEmpty());
  }

  @Test
  void putTableConstraintsAcceptsExplicitEmptyBundle() throws Exception {
    var catName = tablePrefix + "cat_constraints_empty_bundle";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints empty bundle");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_empty_bundle", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_empty_bundle",
            "s3://bucket/fact_constraints_empty_bundle",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tblId = tbl.getResourceId();
    long snapshotId = 3021L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setConstraints(SnapshotConstraints.getDefaultInstance())
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tblId, snapshotId).orElseThrow();
    assertEquals(tblId, stored.getTableId());
    assertEquals(snapshotId, stored.getSnapshotId());
    assertEquals(0, stored.getConstraintsCount());
  }

  @Test
  void putTableConstraintsRejectsUnknownSnapshot() throws Exception {
    var catName = tablePrefix + "cat_constraints_unknown_snapshot";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints unknown snapshot");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_unknown_snapshot", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_unknown_snapshot",
            "s3://bucket/fact_constraints_unknown_snapshot",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tblId = tbl.getResourceId();
    long snapshotId = 9_999L;

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_unknown_snapshot")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraints)
                        .build()));

    TestSupport.assertGrpcAndMc(ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
    assertTrue(constraintRepository.getSnapshotConstraints(tblId, snapshotId).isEmpty());
  }

  @Test
  void putTableConstraintsRejectsIdempotencyReplayWithDifferentPayload() throws Exception {
    var catName = tablePrefix + "cat_constraints_idem";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints idempotency");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_idem", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_idem",
            "s3://bucket/fact_constraints_idem",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    long snapshotId = 404L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    var idem = IdempotencyKey.newBuilder().setKey("stats-constraints-idem").build();

    var constraintsA =
        SnapshotConstraints.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_a")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraintsA)
            .setIdempotency(idem)
            .build());

    var constraintsB =
        constraintsA.toBuilder()
            .clearConstraints()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_b")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraintsB)
                        .setIdempotency(idem)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void putTableConstraintsRejectsUnknownTable() throws Exception {
    var unknownTableId =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId("t-0001")
            .setId("00000000-0000-0000-0000-000000000123")
            .build();
    long snapshotId = 407L;

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(unknownTableId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_unknown_table")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(unknownTableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraints)
                        .build()));

    TestSupport.assertGrpcAndMc(ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
    assertTrue(constraintRepository.getSnapshotConstraints(unknownTableId, snapshotId).isEmpty());
  }

  @Test
  void putTableConstraintsIdempotentReplaySucceeds() throws Exception {
    var catName = tablePrefix + "cat_constraints_replay";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints replay");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_replay", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_replay",
            "s3://bucket/fact_constraints_replay",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    long snapshotId = 405L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_replay")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    var idem = IdempotencyKey.newBuilder().setKey("stats-constraints-replay").build();

    var request =
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .setIdempotency(idem)
            .build();

    var first = constraintsService.putTableConstraints(request);
    var second = constraintsService.putTableConstraints(request);
    assertNotNull(first.getMeta().getPointerKey());
    assertNotNull(second.getMeta().getPointerKey());

    var stored = constraintRepository.getSnapshotConstraints(tblId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    assertEquals("pk_replay", stored.getConstraints(0).getName());
  }

  @Test
  void putTableStatsWithoutConstraintsDoesNotDeleteExistingConstraints() throws Exception {
    var catName = tablePrefix + "cat_constraints_absent";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints absent");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_absent", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_absent",
            "s3://bucket/fact_constraints_absent",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    long snapshotId = 406L;
    TestSupport.createSnapshot(snapshot, tblId, snapshotId, System.currentTimeMillis());

    var withConstraintsStats =
        TableStats.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setRowCount(13)
            .setTotalSizeBytes(130)
            .build();

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_absent")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder()
                            .setColumnId(1)
                            .setColumnName("id")
                            .setOrdinal(1)
                            .build())
                    .build())
            .build();

    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setStats(withConstraintsStats)
            .build());
    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .build());

    // Table stats are create-only for a (table_id, snapshot_id) pair.
    // Replaying the same idempotent PutTableStats request validates that the absence of constraints
    // in stats writes does not remove previously written constraints.
    var replayRequest =
        PutTableStatsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapshotId)
            .setStats(withConstraintsStats)
            .setIdempotency(IdempotencyKey.newBuilder().setKey("stats-only-replay").build())
            .build();
    statistic.putTableStats(replayRequest);
    statistic.putTableStats(replayRequest);

    var stored = constraintRepository.getSnapshotConstraints(tblId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    assertEquals("pk_absent", stored.getConstraints(0).getName());
  }
}
