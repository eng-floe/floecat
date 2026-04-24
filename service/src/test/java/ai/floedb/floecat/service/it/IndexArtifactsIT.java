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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetIndexArtifactRequest;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.MutinyTableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class IndexArtifactsIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes;

  @GrpcClient("floecat")
  MutinyTableIndexServiceGrpc.MutinyTableIndexServiceStub indexesMutiny;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private final String prefix = getClass().getSimpleName() + "_";

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void indexArtifactsPutGetAndListCurrentSnapshot() throws Exception {
    Catalog cat = TestSupport.createCatalog(catalog, prefix + "cat", "cat");
    Namespace ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns", List.of("db_idx", "schema_idx"), "ns");
    Table tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "table");
    ResourceId tableId = tbl.getResourceId();

    long oldSnapshotId = 101L;
    long currentSnapshotId = 202L;
    TestSupport.createSnapshot(
        snapshot, tableId, oldSnapshotId, System.currentTimeMillis() - 1_000L);
    TestSupport.createSnapshot(snapshot, tableId, currentSnapshotId, System.currentTimeMillis());

    IndexArtifactRecord oldRecord =
        artifactRecord(
            tableId,
            oldSnapshotId,
            "s3://bucket/orders/data-000.parquet",
            "s3://bucket/orders/data-000.parquet.floe-index.parquet",
            IndexArtifactState.IAS_READY);
    IndexArtifactRecord currentRecordA =
        artifactRecord(
            tableId,
            currentSnapshotId,
            "s3://bucket/orders/data-001.parquet",
            "s3://bucket/orders/data-001.parquet.floe-index.parquet",
            IndexArtifactState.IAS_READY);
    IndexArtifactRecord currentRecordB =
        artifactRecord(
            tableId,
            currentSnapshotId,
            "s3://bucket/orders/data-002.parquet",
            "s3://bucket/orders/data-002.parquet.floe-index.parquet",
            IndexArtifactState.IAS_PENDING);

    indexesMutiny
        .putIndexArtifacts(
            Multi.createFrom()
                .item(
                    PutIndexArtifactsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(oldSnapshotId)
                        .addRecords(oldRecord)
                        .setIdempotency(
                            IdempotencyKey.newBuilder().setKey("old-snapshot-write").build())
                        .build()))
        .await()
        .indefinitely();

    indexesMutiny
        .putIndexArtifacts(
            Multi.createFrom()
                .item(
                    PutIndexArtifactsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(currentSnapshotId)
                        .addRecords(currentRecordA)
                        .addRecords(currentRecordB)
                        .setIdempotency(
                            IdempotencyKey.newBuilder().setKey("current-snapshot-write").build())
                        .build()))
        .await()
        .indefinitely();

    var getResp =
        indexes.getIndexArtifact(
            GetIndexArtifactRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(currentSnapshotId).build())
                .setTarget(currentRecordA.getTarget())
                .build());
    assertEquals(currentRecordA.getArtifactUri(), getResp.getRecord().getArtifactUri());
    assertEquals(IndexArtifactState.IAS_READY, getResp.getRecord().getState());

    var currentList =
        indexes.listIndexArtifacts(
            ListIndexArtifactsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(
                    SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(2, currentList.getRecordsCount());
    assertEquals(2, currentList.getPage().getTotalSize());

    var oldList =
        indexes.listIndexArtifacts(
            ListIndexArtifactsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(oldSnapshotId).build())
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(1, oldList.getRecordsCount());
    assertEquals(oldRecord.getArtifactUri(), oldList.getRecords(0).getArtifactUri());

    var missing =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                indexes.getIndexArtifact(
                    GetIndexArtifactRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSnapshotId(currentSnapshotId).build())
                        .setTarget(
                            IndexTarget.newBuilder()
                                .setFile(
                                    IndexFileTarget.newBuilder()
                                        .setFilePath("s3://bucket/orders/missing.parquet")
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(missing, Status.Code.NOT_FOUND, null, "not found");
  }

  private static IndexArtifactRecord artifactRecord(
      ResourceId tableId,
      long snapshotId,
      String filePath,
      String artifactUri,
      IndexArtifactState state) {
    return IndexArtifactRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(
            IndexTarget.newBuilder()
                .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
                .build())
        .setArtifactUri(artifactUri)
        .setArtifactFormat("parquet")
        .setArtifactFormatVersion(1)
        .setState(state)
        .setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .setRefreshedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .setSourceFileFormat("parquet")
        .build();
  }
}
