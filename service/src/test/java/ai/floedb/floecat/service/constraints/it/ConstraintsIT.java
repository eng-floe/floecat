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
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
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
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_get")
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotOne)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_list_1")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotTwo)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_list_2")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_delete")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                                .setColumnId(1)
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
                                .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                                        .setColumnId(2)
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
                                        .setColumnId(3)
                                        .setColumnName("email")
                                        .setOrdinal(1)
                                        .build())
                                .build())
                        .build())
                .build());

    assertEquals(2, merged.getConstraints().getConstraintsCount());
    assertEquals(
        2L,
        merged.getConstraints().getConstraintsList().stream()
            .filter(c -> c.getName().equals("pk_existing"))
            .findFirst()
            .orElseThrow()
            .getColumns(0)
            .getColumnId());
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
                                        .setColumnId(1)
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
                                    .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_existing")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                                                .setColumnId(2)
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
                                    .setColumnId(1)
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
                                        .setColumnId(1)
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
                                .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_keep")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
                                    .setColumnId(1)
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
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .addConstraints(
                            ConstraintDefinition.newBuilder()
                                .setName("pk_initial")
                                .setType(ConstraintType.CT_PRIMARY_KEY)
                                .addColumns(
                                    ConstraintColumnRef.newBuilder()
                                        .setColumnId(1)
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
                                        .setColumnId(1)
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
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addConstraints(
                        ConstraintDefinition.newBuilder()
                            .setName("pk_only")
                            .setType(ConstraintType.CT_PRIMARY_KEY)
                            .addColumns(
                                ConstraintColumnRef.newBuilder()
                                    .setColumnId(1)
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
