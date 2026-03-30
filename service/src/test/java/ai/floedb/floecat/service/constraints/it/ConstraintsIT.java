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
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
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
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticMutiny;

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

    var tableId = tbl.getResourceId();
    long snapshotId = 303L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tableId.toBuilder().setId("wrong-id-should-be-overwritten").build())
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_fact_constraints")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(tableId, stored.getTableId());
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

    var tableId = tbl.getResourceId();
    long snapshotId = 302L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "constraints");
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
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

    var tableId = tbl.getResourceId();
    long snapshotId = 3021L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(SnapshotConstraints.getDefaultInstance())
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(tableId, stored.getTableId());
    assertEquals(snapshotId, stored.getSnapshotId());
    assertEquals(0, stored.getConstraintsCount());
  }

  @Test
  void putTableConstraintsRejectsBlankConstraintName() throws Exception {
    var catName = tablePrefix + "cat_constraints_blank_name_put";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for constraints blank name");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_blank_name_put", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_blank_name_put",
            "s3://bucket/fact_constraints_blank_name_put",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotId = 3022L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("   ")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
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

    var tableId = tbl.getResourceId();
    long snapshotId = 9_999L;

    var constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_unknown_snapshot")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraints)
                        .build()));

    TestSupport.assertGrpcAndMc(ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
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
    var tableId = tbl.getResourceId();
    long snapshotId = 404L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var idem = IdempotencyKey.newBuilder().setKey("stats-constraints-idem").build();

    var constraintsA =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_a")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
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
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraintsB)
                        .setIdempotency(idem)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void putTableConstraintsRejectsUnknownTable() throws Exception {
    var tableId =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId("t-0001")
            .setId("00000000-0000-0000-0000-000000000123")
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();
    long snapshotId = 407L;

    var constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_unknown_table")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(constraints)
                        .build()));

    TestSupport.assertGrpcAndMc(ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
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
    var tableId = tbl.getResourceId();
    long snapshotId = 405L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_replay")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    var idem = IdempotencyKey.newBuilder().setKey("stats-constraints-replay").build();

    var request =
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .setIdempotency(idem)
            .build();

    var first = constraintsService.putTableConstraints(request);
    var second = constraintsService.putTableConstraints(request);
    assertNotNull(first.getMeta().getPointerKey());
    assertNotNull(second.getMeta().getPointerKey());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
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
    var tableId = tbl.getResourceId();
    long snapshotId = 406L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var withConstraintsStats =
        TableValueStats.newBuilder().setRowCount(13).setTotalSizeBytes(130).build();

    var constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_absent")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();

    statisticMutiny
        .putTargetStats(
            io.smallrye.mutiny.Multi.createFrom()
                .item(
                    PutTargetStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .addRecords(
                            TargetStatsRecord.newBuilder()
                                .setTableId(tableId)
                                .setSnapshotId(snapshotId)
                                .setTarget(
                                    StatsTarget.newBuilder()
                                        .setTable(TableStatsTarget.newBuilder().build())
                                        .build())
                                .setTable(
                                    TableValueStats.newBuilder()
                                        .setRowCount(withConstraintsStats.getRowCount())
                                        .setDataFileCount(withConstraintsStats.getDataFileCount())
                                        .setTotalSizeBytes(withConstraintsStats.getTotalSizeBytes())
                                        .build())
                                .build())
                        .build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));
    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(constraints)
            .build());

    // Table stats are create-only for a (table_id, snapshot_id) pair.
    // Replaying the same idempotent PutTargetStats request validates that the absence of
    // constraints
    // in stats writes does not remove previously written constraints.
    var replayRequest =
        PutTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .addRecords(
                TargetStatsRecord.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .setTarget(
                        StatsTarget.newBuilder()
                            .setTable(TableStatsTarget.newBuilder().build())
                            .build())
                    .setTable(
                        TableValueStats.newBuilder()
                            .setRowCount(withConstraintsStats.getRowCount())
                            .setDataFileCount(withConstraintsStats.getDataFileCount())
                            .setTotalSizeBytes(withConstraintsStats.getTotalSizeBytes())
                            .build())
                    .build())
            .setIdempotency(IdempotencyKey.newBuilder().setKey("stats-only-replay").build())
            .build();
    statisticMutiny
        .putTargetStats(io.smallrye.mutiny.Multi.createFrom().item(replayRequest))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));
    statisticMutiny
        .putTargetStats(io.smallrye.mutiny.Multi.createFrom().item(replayRequest))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    assertEquals("pk_absent", stored.getConstraints(0).getName());
  }

  @Test
  void getTableConstraintsReturnsStoredBundle() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_get", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_get", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_get",
            "s3://bucket/fact_constraints_get",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotId = 501L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var bundle =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_get")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .addColumns(
                        ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
                    .build())
            .build();
    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(bundle)
            .build());

    var response =
        constraintsService.getTableConstraints(
            GetTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .build());
    assertEquals(tableId, response.getConstraints().getTableId());
    assertEquals(snapshotId, response.getConstraints().getSnapshotId());
    assertEquals(1, response.getConstraints().getConstraintsCount());
    assertEquals("pk_get", response.getConstraints().getConstraints(0).getName());
    assertTrue(response.hasMeta());
  }

  @Test
  void listTableConstraintsReturnsPagedSnapshotBundles() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_list", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_list", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_list",
            "s3://bucket/fact_constraints_list",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotOne = 510L;
    long snapshotTwo = 511L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotOne, System.currentTimeMillis());
    TestSupport.createSnapshot(snapshot, tableId, snapshotTwo, System.currentTimeMillis() + 1);

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotOne)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_list_1")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());
    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotTwo)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_list_2")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var pageOne =
        constraintsService.listTableConstraints(
            ListTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setPage(
                    ai.floedb.floecat.common.rpc.PageRequest.newBuilder().setPageSize(1).build())
                .build());
    assertEquals(1, pageOne.getConstraintsCount());
    assertFalse(pageOne.getPage().getNextPageToken().isBlank());
    assertEquals(2, pageOne.getPage().getTotalSize());

    var pageTwo =
        constraintsService.listTableConstraints(
            ListTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setPage(
                    ai.floedb.floecat.common.rpc.PageRequest.newBuilder()
                        .setPageSize(1)
                        .setPageToken(pageOne.getPage().getNextPageToken())
                        .build())
                .build());
    assertEquals(1, pageTwo.getConstraintsCount());
  }

  @Test
  void deleteTableConstraintsRemovesStoredBundle() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_delete", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_delete", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_delete",
            "s3://bucket/fact_constraints_delete",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotId = 520L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());
    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_delete")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var response =
        constraintsService.deleteTableConstraints(
            DeleteTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .build());
    assertTrue(response.hasMeta());
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
  }

  @Test
  void addTableConstraintAddsSingleConstraintWithoutReplacingBundle() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_add_one_preserve", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_add_one_preserve", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_add_one_preserve",
            "s3://bucket/fact_constraints_add_one_preserve",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 700L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var addResponse =
        constraintsService.addTableConstraint(
            AddTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraint(
                    ConstraintDefinition.newBuilder()
                        .setName("uq_added")
                        .setType(ConstraintType.CT_UNIQUE)
                        .addColumns(
                            ConstraintColumnRef.newBuilder()
                                .setColumnName("id")
                                .setOrdinal(1)
                                .build())
                        .build())
                .build());
    assertEquals(2, addResponse.getConstraints().getConstraintsCount());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(2, stored.getConstraintsCount());
    assertTrue(
        stored.getConstraintsList().stream().anyMatch(c -> c.getName().equals("pk_existing")));
    assertTrue(stored.getConstraintsList().stream().anyMatch(c -> c.getName().equals("uq_added")));
  }

  @Test
  void addTableConstraintTrimsNameAndDeleteMatchesCanonicalName() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_add_one_trimmed_name", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_add_one_trimmed_name",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_add_one_trimmed_name",
            "s3://bucket/fact_constraints_add_one_trimmed_name",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7007L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var addResponse =
        constraintsService.addTableConstraint(
            AddTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraint(
                    ConstraintDefinition.newBuilder()
                        .setName("  uq_trimmed  ")
                        .setType(ConstraintType.CT_UNIQUE)
                        .addColumns(
                            ConstraintColumnRef.newBuilder()
                                .setColumnName("id")
                                .setOrdinal(1)
                                .build())
                        .build())
                .build());
    assertEquals(1, addResponse.getConstraints().getConstraintsCount());
    assertEquals("uq_trimmed", addResponse.getConstraints().getConstraints(0).getName());

    var deleteResponse =
        constraintsService.deleteTableConstraint(
            DeleteTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraintName("uq_trimmed")
                .build());
    assertEquals(0, deleteResponse.getConstraints().getConstraintsCount());
  }

  @Test
  void mergeTableConstraintsMergesByNameWithoutReplacingBundle() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_merge_bundle", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_merge_bundle", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_merge_bundle",
            "s3://bucket/fact_constraints_merge_bundle",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7001L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var merged =
        constraintsService.mergeTableConstraints(
            MergeTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(
                    SnapshotConstraints.newBuilder()
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("pk_existing")
                                .setType(ConstraintType.CT_PRIMARY_KEY)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("id2")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("uq_added")
                                .setType(ConstraintType.CT_UNIQUE)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("email")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build())
                .build());

    assertEquals(2, merged.getConstraints().getConstraintsCount());
    assertEquals(
        "id2",
        merged.getConstraints().getConstraintsList().stream()
            .filter(c -> c.getName().equals("pk_existing"))
            .findFirst()
            .orElseThrow()
            .getColumns(0)
            .getColumnName());
    assertEquals(
        1,
        merged.getConstraints().getConstraintsList().stream()
            .filter(c -> c.getName().equals("uq_added"))
            .count());
  }

  @Test
  void mergeTableConstraintsCreatesBundleWhenMissing() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_merge_create", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_merge_create", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_merge_create",
            "s3://bucket/fact_constraints_merge_create",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7004L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var merged =
        constraintsService.mergeTableConstraints(
            MergeTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(
                    SnapshotConstraints.newBuilder()
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("pk_created")
                                .setType(ConstraintType.CT_PRIMARY_KEY)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("id")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build())
                .build());

    assertEquals(1, merged.getConstraints().getConstraintsCount());
    assertEquals("pk_created", merged.getConstraints().getConstraints(0).getName());
  }

  @Test
  void mergeTableConstraintsRejectsBlankConstraintName() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_merge_blank", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_merge_blank", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_merge_blank",
            "s3://bucket/fact_constraints_merge_blank",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7005L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.mergeTableConstraints(
                    MergeTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
  }

  @Test
  void mergeTableConstraintsRejectsDuplicateConstraintNameAfterTrim() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_merge_duplicate_trim", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_merge_duplicate_trim",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_merge_duplicate_trim",
            "s3://bucket/fact_constraints_merge_duplicate_trim",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7009L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.mergeTableConstraints(
                    MergeTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_new")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("  pk_new ")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
  }

  @Test
  void appendTableConstraintsRejectsDuplicateConstraintName() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_append_duplicate", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_append_duplicate", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_append_duplicate",
            "s3://bucket/fact_constraints_append_duplicate",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7002L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.appendTableConstraints(
                    AppendTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_existing")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .addColumns(
                                            ConstraintColumnRef.newBuilder()
                                                .setColumnName("id2")
                                                .setOrdinal(1)
                                                .build())
                                        .build())
                                .build())
                        .build()));

    assertEquals(Status.Code.ALREADY_EXISTS, ex.getStatus().getCode());
  }

  @Test
  void appendTableConstraintsRejectsDuplicateConstraintNameAfterTrim() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_append_duplicate_trim", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_append_duplicate_trim",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_append_duplicate_trim",
            "s3://bucket/fact_constraints_append_duplicate_trim",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7008L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.appendTableConstraints(
                    AppendTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("  pk_existing ")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .build())
                        .build()));

    assertEquals(Status.Code.ALREADY_EXISTS, ex.getStatus().getCode());
  }

  @Test
  void appendTableConstraintsCreatesBundleWhenMissing() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_append_create", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_append_create", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_append_create",
            "s3://bucket/fact_constraints_append_create",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7003L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var appended =
        constraintsService.appendTableConstraints(
            AppendTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(
                    SnapshotConstraints.newBuilder()
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("pk_created")
                                .setType(ConstraintType.CT_PRIMARY_KEY)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("id")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build())
                .build());

    assertEquals(1, appended.getConstraints().getConstraintsCount());
    assertEquals("pk_created", appended.getConstraints().getConstraints(0).getName());
  }

  @Test
  void appendTableConstraintsRejectsBlankConstraintName() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_append_blank", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_append_blank", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_append_blank",
            "s3://bucket/fact_constraints_append_blank",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7006L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.appendTableConstraints(
                    AppendTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName(" ")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
  }

  @Test
  void putTableConstraintsRejectsDuplicateNamesInPayload() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_put_dup", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_put_dup", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_put_dup",
            "s3://bucket/fact_constraints_put_dup",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7010L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_dup")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_dup")
                                        .setType(ConstraintType.CT_UNIQUE)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
  }

  @Test
  void appendTableConstraintsRejectsDuplicateNamesInSamePayload() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_append_dup_payload", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_append_dup_payload",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_append_dup_payload",
            "s3://bucket/fact_constraints_append_dup_payload",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7011L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.appendTableConstraints(
                    AppendTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_dup")
                                        .setType(ConstraintType.CT_PRIMARY_KEY)
                                        .build())
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("pk_dup")
                                        .setType(ConstraintType.CT_UNIQUE)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "constraints.constraints.name");
  }

  @Test
  void addTableConstraintCreatesBundleWhenMissing() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_add_one_create", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_add_one_create", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_add_one_create",
            "s3://bucket/fact_constraints_add_one_create",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 701L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var response =
        constraintsService.addTableConstraint(
            AddTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraint(
                    ConstraintDefinition.newBuilder()
                        .setName("pk_created")
                        .setType(ConstraintType.CT_PRIMARY_KEY)
                        .addColumns(
                            ConstraintColumnRef.newBuilder()
                                .setColumnName("id")
                                .setOrdinal(1)
                                .build())
                        .build())
                .build());
    assertEquals(1, response.getConstraints().getConstraintsCount());
    assertEquals("pk_created", response.getConstraints().getConstraints(0).getName());
  }

  @Test
  void deleteTableConstraintRemovesSingleConstraintWithoutReplacingBundle() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_delete_one_preserve", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_delete_one_preserve",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_delete_one_preserve",
            "s3://bucket/fact_constraints_delete_one_preserve",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 702L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_keep")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("uq_drop")
                            .setType(ConstraintType.CT_UNIQUE)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var response =
        constraintsService.deleteTableConstraint(
            DeleteTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraintName("uq_drop")
                .build());
    assertEquals(1, response.getConstraints().getConstraintsCount());
    assertEquals("pk_keep", response.getConstraints().getConstraints(0).getName());
  }

  @Test
  void addTableConstraintHonorsPrecondition() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_add_one_pre", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_add_one_pre", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_add_one_pre",
            "s3://bucket/fact_constraints_add_one_pre",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 703L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var putResponse =
        constraintsService.putTableConstraints(
            PutTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(
                    SnapshotConstraints.newBuilder()
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("pk_initial")
                                .setType(ConstraintType.CT_PRIMARY_KEY)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("id")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build())
                .build());

    var stalePrecondition =
        ai.floedb.floecat.common.rpc.Precondition.newBuilder()
            .setExpectedVersion(putResponse.getMeta().getPointerVersion() - 1)
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.addTableConstraint(
                    AddTableConstraintRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setPrecondition(stalePrecondition)
                        .setConstraint(
                            ConstraintDefinition.newBuilder()
                                .setName("uq_new")
                                .setType(ConstraintType.CT_UNIQUE)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnName("id")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build()));
    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
  }

  @Test
  void deleteTableConstraintMissingNameIsNotFound() throws Exception {
    var cat =
        TestSupport.createCatalog(
            catalog, tablePrefix + "cat_constraints_delete_one_missing", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace,
            cat.getResourceId(),
            "it_ns_constraints_delete_one_missing",
            List.of(),
            "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_delete_one_missing",
            "s3://bucket/fact_constraints_delete_one_missing",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 704L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_only")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("id")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.deleteTableConstraint(
                    DeleteTableConstraintRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraintName("does_not_exist")
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.NOT_FOUND,
        ErrorCode.MC_NOT_FOUND,
        "Constraint \"does_not_exist\" not found for table");
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    assertTrue(ex.getStatus().getDescription().contains("does_not_exist"));
    assertTrue(ex.getStatus().getDescription().contains(Long.toString(snapshotId)));
    assertTrue(ex.getStatus().getDescription().contains(tableId.getId()));
  }

  @Test
  void getTableConstraintsMissingBundleIsNotFound() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_get_missing", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_get_missing", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_get_missing",
            "s3://bucket/fact_constraints_get_missing",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotId = 530L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.getTableConstraints(
                    GetTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Table constraints not found for table");
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    assertTrue(ex.getStatus().getDescription().contains("Table constraints not found"));
    assertTrue(ex.getStatus().getDescription().contains(Long.toString(snapshotId)));
  }

  @Test
  void deleteTableConstraintsMissingBundleIsNotFound() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_delete_missing", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_delete_missing", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_delete_missing",
            "s3://bucket/fact_constraints_delete_missing",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    long snapshotId = 531L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.deleteTableConstraints(
                    DeleteTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Table constraints not found for table");
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    assertTrue(ex.getStatus().getDescription().contains("Table constraints not found"));
    assertTrue(ex.getStatus().getDescription().contains(Long.toString(snapshotId)));
  }

  @Test
  void listTableConstraintsEmptyReturnsNoRows() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_list_empty", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_list_empty", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_list_empty",
            "s3://bucket/fact_constraints_list_empty",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    var response =
        constraintsService.listTableConstraints(
            ListTableConstraintsRequest.newBuilder().setTableId(tableId).build());
    assertEquals(0, response.getConstraintsCount());
    assertEquals(0, response.getPage().getTotalSize());
    assertTrue(response.getPage().getNextPageToken().isBlank());
  }

  /** Shared setup for constraint IT tests: creates a catalog, namespace, table, and snapshot. */
  record ConstraintTestContext(ResourceId tableId, long snapshotId, String catalogName) {}

  private ConstraintTestContext setupConstraintTable(String suffix, long snapshotId) {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_" + suffix, "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_" + suffix, List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_" + suffix,
            "s3://bucket/fact_constraints_" + suffix,
            "{\"cols\":[{\"name\":\"id\",\"type\":\"bigint\"}]}",
            "none");
    TestSupport.createSnapshot(
        snapshot, tbl.getResourceId(), snapshotId, System.currentTimeMillis());
    return new ConstraintTestContext(tbl.getResourceId(), snapshotId, cat.getDisplayName());
  }

  @Test
  void putTableConstraintsWithNotNullConstraint() throws Exception {
    var ctx = setupConstraintTable("nn", 7012L);
    var tableId = ctx.tableId();
    long snapshotId = ctx.snapshotId();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("nn_email")
                            .setType(ConstraintType.CT_NOT_NULL)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("email")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    var c = stored.getConstraints(0);
    assertEquals("nn_email", c.getName());
    assertEquals(ConstraintType.CT_NOT_NULL, c.getType());
    assertEquals(1, c.getColumnsCount());
    assertEquals("email", c.getColumns(0).getColumnName());
  }

  @Test
  void putTableConstraintsWithCheckConstraint() throws Exception {
    var ctx = setupConstraintTable("check", 7013L);
    var tableId = ctx.tableId();
    long snapshotId = ctx.snapshotId();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("chk_age_positive")
                            .setType(ConstraintType.CT_CHECK)
                            .setCheckExpression("age > 0")
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("age")
                                    .setOrdinal(1)
                                    .build())
                            .build())
                    .build())
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    var c = stored.getConstraints(0);
    assertEquals("chk_age_positive", c.getName());
    assertEquals(ConstraintType.CT_CHECK, c.getType());
    assertEquals("age > 0", c.getCheckExpression());
    assertEquals(1, c.getColumnsCount());
    assertEquals("age", c.getColumns(0).getColumnName());
  }

  @Test
  void putTableConstraintsWithMultiColumnFkAndReferentialActions() throws Exception {
    var ctx = setupConstraintTable("fk_multi", 7014L);
    var tableId = ctx.tableId();
    long snapshotId = ctx.snapshotId();

    constraintsService.putTableConstraints(
        PutTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraints(
                SnapshotConstraints.newBuilder()
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("fk_multi_col")
                            .setType(ConstraintType.CT_FOREIGN_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("a")
                                    .setOrdinal(1)
                                    .build())
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("b")
                                    .setOrdinal(2)
                                    .build())
                            .addReferencedColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("ref_a")
                                    .setOrdinal(1)
                                    .build())
                            .addReferencedColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnName("ref_b")
                                    .setOrdinal(2)
                                    .build())
                            .setReferencedTable(NameRef.newBuilder().setName("ref_table").build())
                            .setReferencedConstraintName("pk_ref")
                            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
                            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
                            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION)
                            .build())
                    .build())
            .build());

    var stored = constraintRepository.getSnapshotConstraints(tableId, snapshotId).orElseThrow();
    assertEquals(1, stored.getConstraintsCount());
    var c = stored.getConstraints(0);
    assertEquals("fk_multi_col", c.getName());
    assertEquals(ConstraintType.CT_FOREIGN_KEY, c.getType());
    assertEquals(2, c.getColumnsCount());
    assertEquals(2, c.getReferencedColumnsCount());
    assertEquals("ref_a", c.getReferencedColumns(0).getColumnName());
    assertEquals("ref_b", c.getReferencedColumns(1).getColumnName());
    assertEquals("pk_ref", c.getReferencedConstraintName());
    assertEquals(ctx.catalogName(), c.getReferencedTable().getCatalog());
    assertEquals(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL, c.getMatchOption());
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE, c.getDeleteRule());
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION, c.getUpdateRule());
  }

  @Test
  void putTableConstraintsRejectsFkColumnCountMismatch() throws Exception {
    var ctx = setupConstraintTable("fk_mismatch", 7015L);
    var tableId = ctx.tableId();
    long snapshotId = ctx.snapshotId();

    // FK with 2 local columns but only 1 referenced column — invalid.
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("fk_mismatch")
                                        .setType(ConstraintType.CT_FOREIGN_KEY)
                                        .addColumns(
                                            ConstraintColumnRef.newBuilder()
                                                .setColumnName("a")
                                                .setOrdinal(1)
                                                .build())
                                        .addColumns(
                                            ConstraintColumnRef.newBuilder()
                                                .setColumnName("b")
                                                .setOrdinal(2)
                                                .build())
                                        .addReferencedColumns(
                                            ConstraintColumnRef.newBuilder()
                                                .setColumnName("ref_a")
                                                .setOrdinal(1)
                                                .build())
                                        .setReferencedTable(
                                            NameRef.newBuilder().setName("ref_table").build())
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "referenced_columns");
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
  }

  @Test
  void putTableConstraintsRejectsFkOneSidedColumns() throws Exception {
    var ctx = setupConstraintTable("fk_onesided", 7016L);
    var tableId = ctx.tableId();
    long snapshotId = ctx.snapshotId();

    // FK with local columns only (referenced_columns absent) — one-sided, always invalid.
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.putTableConstraints(
                    PutTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setConstraints(
                            SnapshotConstraints.newBuilder()
                                .addConstraints(
                                    ConstraintDefinition.newBuilder()
                                        .setName("fk_onesided")
                                        .setType(ConstraintType.CT_FOREIGN_KEY)
                                        .addColumns(
                                            ConstraintColumnRef.newBuilder()
                                                .setColumnName("a")
                                                .setOrdinal(1)
                                                .build())
                                        // referenced_columns intentionally absent
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "referenced_columns");
    assertTrue(constraintRepository.getSnapshotConstraints(tableId, snapshotId).isEmpty());
  }

  @Test
  void listTableConstraintsRejectsInvalidPageToken() throws Exception {
    var cat =
        TestSupport.createCatalog(catalog, tablePrefix + "cat_constraints_list_bad_token", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_constraints_list_bad_token", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_constraints_list_bad_token",
            "s3://bucket/fact_constraints_list_bad_token",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");

    var tableId = tbl.getResourceId();
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                constraintsService.listTableConstraints(
                    ListTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setPage(
                            ai.floedb.floecat.common.rpc.PageRequest.newBuilder()
                                .setPageSize(5)
                                .setPageToken("not-a-valid-token")
                                .build())
                        .build()));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }
}
