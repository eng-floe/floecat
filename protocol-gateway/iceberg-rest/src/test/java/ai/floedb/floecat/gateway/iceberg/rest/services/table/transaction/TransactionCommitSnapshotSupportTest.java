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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.storage.kv.Keys;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitSnapshotSupportTest {

  @Test
  void planAtomicSnapshotChangesPrefersMaterializedMetadataLocationOverRequestSnapshotLocation()
      throws Exception {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    List<Map<String, Object>> updates =
        List.of(
            Map.of(
                "action",
                "add-snapshot",
                "snapshot",
                Map.of(
                    "snapshot-id",
                    4652753989274070009L,
                    "timestamp-ms",
                    1781027618000L,
                    "metadata-location",
                    "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00000-request.metadata.json",
                    "summary",
                    Map.of("operation", "append"))));

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            Table.newBuilder().setResourceId(tableId).build(),
            null,
            "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00002-materialized.metadata.json",
            updates,
            List.of());

    assertNull(result.error());
    assertEquals(3, result.txChanges().size());
    Snapshot snapshot = Snapshot.parseFrom(result.txChanges().getFirst().getPayload());
    assertEquals(
        "s3://floecat/iceberg/trino_fmt_v1_smoke/metadata/00002-materialized.metadata.json",
        snapshot.getMetadataLocation());
    assertTrue(snapshot.hasMetadataLocation());
    var currentPointerChange = result.txChanges().get(2);
    assertEquals(
        "/accounts/acct-1/tables/tbl-1/snapshots/current",
        currentPointerChange.getTargetPointerKey());
    CurrentSnapshotPointer current =
        CurrentSnapshotPointer.parseFrom(currentPointerChange.getPayload());
    assertEquals(tableId, current.getTableId());
    assertEquals(4652753989274070009L, current.getSnapshotId());
    assertTrue(current.hasUpstreamCreatedAt());
  }

  @Test
  void planAtomicSnapshotChangesWritesCurrentPointerForExistingMainSnapshot() throws Exception {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    Snapshot existingSnapshot =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(1234L).build();
    Mockito.when(support.grpcClient.getSnapshot(Mockito.any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(existingSnapshot).build());

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            Table.newBuilder().setResourceId(tableId).build(),
            null,
            null,
            List.of(
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    1234L,
                    "type",
                    "branch")),
            List.of());

    assertNull(result.error());
    assertEquals(1, result.txChanges().size());
    assertEquals(
        "/accounts/acct-1/tables/tbl-1/snapshots/current",
        result.txChanges().getFirst().getTargetPointerKey());
    CurrentSnapshotPointer current =
        CurrentSnapshotPointer.parseFrom(result.txChanges().getFirst().getPayload());
    assertEquals(tableId, current.getTableId());
    assertEquals(1234L, current.getSnapshotId());
  }

  @Test
  void planAtomicSnapshotChangesIgnoresStaleMainRefWithoutAddedSnapshot() {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    Mockito.when(tableSupport.loadCurrentSnapshotId(table)).thenReturn(2000L);

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            table,
            tableSupport,
            null,
            List.of(
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    1000L,
                    "type",
                    "branch")),
            List.of());

    assertNull(result.error());
    assertTrue(result.txChanges().isEmpty());
  }

  @Test
  void planAtomicSnapshotChangesDoesNotMoveCurrentForAddedSnapshotWithoutMainRef() {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    Mockito.when(tableSupport.loadCurrentSnapshotId(table)).thenReturn(2000L);

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            table,
            tableSupport,
            null,
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        1000L,
                        "timestamp-ms",
                        1781027618000L,
                        "summary",
                        Map.of("operation", "append")))),
            List.of());

    assertNull(result.error());
    assertEquals(2, result.txChanges().size());
    for (var change : result.txChanges()) {
      assertTrue(!change.getTargetPointerKey().endsWith("/snapshots/current"));
    }
  }

  @Test
  void planAtomicSnapshotChangesRewritesCurrentSnapshotMetadataForMetadataOnlyCommit()
      throws Exception {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    Snapshot existingSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(777L)
            .setMetadataLocation("s3://bucket/metadata/00001.metadata.json")
            .build();
    Mockito.when(support.grpcClient.getSnapshot(Mockito.any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(existingSnapshot).build());

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            table,
            null,
            "s3://bucket/metadata/00002.metadata.json",
            List.of(Map.of("action", "set-properties", "updates", Map.of("k", "v"))),
            List.of());

    assertNull(result.error());
    assertEquals(2, result.txChanges().size());
    Snapshot byId = Snapshot.parseFrom(result.txChanges().getFirst().getPayload());
    assertEquals("s3://bucket/metadata/00002.metadata.json", byId.getMetadataLocation());
    Snapshot byTime = Snapshot.parseFrom(result.txChanges().get(1).getPayload());
    assertEquals("s3://bucket/metadata/00002.metadata.json", byTime.getMetadataLocation());
  }

  @Test
  void planAtomicSnapshotChangesRewritesCurrentSnapshotMetadataForEmptyCommit() throws Exception {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    Snapshot existingSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(777L)
            .setMetadataLocation("s3://bucket/metadata/00001.metadata.json")
            .build();
    Mockito.when(support.grpcClient.getSnapshot(Mockito.any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(existingSnapshot).build());

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            table,
            null,
            "s3://bucket/metadata/00002.metadata.json",
            List.of(),
            List.of());

    assertNull(result.error());
    assertEquals(2, result.txChanges().size());
    Snapshot byId = Snapshot.parseFrom(result.txChanges().getFirst().getPayload());
    assertEquals("s3://bucket/metadata/00002.metadata.json", byId.getMetadataLocation());
    Snapshot byTime = Snapshot.parseFrom(result.txChanges().get(1).getPayload());
    assertEquals("s3://bucket/metadata/00002.metadata.json", byTime.getMetadataLocation());
  }

  @Test
  void planAtomicSnapshotChangesDeletesRemovedSnapshotUsingStoredByTimeTimestamp() {
    TransactionCommitSnapshotSupport support = new TransactionCommitSnapshotSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);
    support.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    long removedSnapshotId = 777L;
    Snapshot listedSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(removedSnapshotId)
            .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(1234L))
            .build();
    Snapshot byIdSnapshot =
        Snapshot.newBuilder().setTableId(tableId).setSnapshotId(removedSnapshotId).build();

    Mockito.when(support.grpcClient.listSnapshots(Mockito.any()))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(listedSnapshot).build());
    Mockito.when(support.grpcClient.getSnapshot(Mockito.any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(byIdSnapshot).build());

    var result =
        support.planAtomicSnapshotChanges(
            "acct-1",
            "tx-1",
            tableId,
            Table.newBuilder().setResourceId(tableId).build(),
            null,
            null,
            List.of(),
            List.of(removedSnapshotId));

    assertNull(result.error());
    assertEquals(2, result.txChanges().size());
    assertEquals(
        "/accounts/acct-1/tables/tbl-1/snapshots/by-id/0000000000000000777",
        result.txChanges().get(0).getTargetPointerKey());
    assertEquals(
        Keys.snapshotPointerByTime("acct-1", "tbl-1", removedSnapshotId, 1234L),
        result.txChanges().get(1).getTargetPointerKey());
  }
}
