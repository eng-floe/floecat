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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.resources.AbstractRestResourceTest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.RestResourceTestProfile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergEncryptedKey;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.google.protobuf.ByteString;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class TableCommitResourceTest extends AbstractRestResourceTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();
  private static final String FIXTURE_LOCATION =
      Objects.requireNonNull(
          FIXTURE.table().getPropertiesMap().get("location"), "fixture location is required");

  private Table.Builder baseTable(ResourceId tableId, ResourceId nsId) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setCatalogId(ResourceId.newBuilder().setId("cat"))
        .setNamespaceId(nsId)
        .putAllProperties(FIXTURE.table().getPropertiesMap());
  }

  @Test
  void commitSupportsSetLocationUpdate() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .setUri("s3://bucket/path/")
            .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
            .build();
    Table current = baseTable(tableId, nsId).setUpstream(upstream).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    UpstreamRef updatedUpstream = upstream.toBuilder().setUri("s3://bucket/new_path/").build();
    Table updated = Table.newBuilder(current).setUpstream(updatedUpstream).build();
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(updated).build());

    given()
        .body(
            "{\"requirements\":[],\"updates\":[{\"action\":\"set-location\",\"location\":\"s3://bucket/new_path/\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200)
        .body("metadata.location", equalTo("s3://bucket/new_path/"));

    ArgumentCaptor<UpdateTableRequest> request = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableStub, atLeastOnce()).updateTable(request.capture());
    assertEquals(
        "s3://bucket/new_path/", request.getAllValues().get(0).getSpec().getUpstream().getUri());
  }

  @Test
  void commitAddSnapshotCreatesPlaceholderAndTriggersReconcile() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Snapshot newSnapshot = FIXTURE.snapshots().get(FIXTURE.snapshots().size() - 1);

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .setUri(FIXTURE_LOCATION)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        baseTable(tableId, nsId)
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .setUpstream(upstream)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());
    int schemaId = FIXTURE.metadata().getCurrentSchemaId();
    String schemaJson = FIXTURE.table().getSchemaJson().replace("\"", "\\\"");

    given()
        .body(
            String.format(
                """
                {"requirements":[],"updates":[{"action":"add-snapshot","snapshot":{
                  "snapshot-id":%d,
                  "timestamp-ms":1000,
                  "parent-snapshot-id":%d,
                  "manifest-list":"%s",
                  "schema-id":%d,
                  "schema-json":"%s",
                  "summary":{"operation":"append"}
                }}]}
                """,
                newSnapshot.getSnapshotId(),
                newSnapshot.getParentSnapshotId(),
                newSnapshot.getManifestList(),
                schemaId,
                schemaJson))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<CreateSnapshotRequest> snapReq =
        ArgumentCaptor.forClass(CreateSnapshotRequest.class);
    verify(snapshotStub).createSnapshot(snapReq.capture());
    assertEquals(newSnapshot.getSnapshotId(), snapReq.getValue().getSpec().getSnapshotId());
    verify(connectorsStub).triggerReconcile(any());
  }

  @Test
  void commitRemoveSnapshotsCallsDelete() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        baseTable(tableId, nsId)
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    given()
        .body(
            "{\"requirements\":[],\"updates\":[{\"action\":\"remove-snapshots\",\"snapshot-ids\":[7,8]}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<DeleteSnapshotRequest> delReq =
        ArgumentCaptor.forClass(DeleteSnapshotRequest.class);
    verify(snapshotStub, times(2)).deleteSnapshot(delReq.capture());
    assertEquals(7L, delReq.getAllValues().get(0).getSnapshotId());
    assertEquals(8L, delReq.getAllValues().get(1).getSnapshotId());
    verify(connectorsStub, never()).triggerReconcile(any());
  }

  @Test
  void commitSetSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    List<Snapshot> fixtureSnapshots = FIXTURE.snapshots();
    long existingSnapshotId = fixtureSnapshots.get(0).getSnapshotId();
    long newSnapshotId = fixtureSnapshots.get(fixtureSnapshots.size() - 1).getSnapshotId();

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .setUri(FIXTURE_LOCATION)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        baseTable(tableId, nsId)
            .setUpstream(upstream)
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("current-snapshot-id", Long.toString(existingSnapshotId))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(existingSnapshotId)
            .putFormatMetadata(
                "iceberg",
                FIXTURE.metadata().toBuilder()
                    .setCurrentSnapshotId(existingSnapshotId)
                    .putRefs(
                        "main",
                        IcebergRef.newBuilder()
                            .setType("branch")
                            .setSnapshotId(existingSnapshotId)
                            .build())
                    .build()
                    .toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());
    int schemaId = FIXTURE.metadata().getCurrentSchemaId();
    String schemaJson = FIXTURE.table().getSchemaJson().replace("\"", "\\\"");

    given()
        .body(
            String.format(
                """
                {"requirements":[],"updates":[
                  {"action":"add-snapshot","snapshot":{
                    "snapshot-id":%d,
                    "timestamp-ms":1000,
                    "manifest-list":"%s",
                    "schema-id":%d,
                    "schema-json":"%s",
                    "summary":{"operation":"append"}
                  }},
                  {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":%d}
                ]}
                """,
                newSnapshotId,
                fixtureSnapshots.get(fixtureSnapshots.size() - 1).getManifestList(),
                schemaId,
                schemaJson,
                newSnapshotId))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub, atLeastOnce()).updateSnapshot(updateReq.capture());
    boolean updatedMainRef =
        updateReq.getAllValues().stream()
            .map(UpdateSnapshotRequest::getSpec)
            .map(spec -> metadataFromSpec(spec).getRefsOrThrow("main").getSnapshotId())
            .anyMatch(snapshotId -> snapshotId == newSnapshotId);
    assertTrue(updatedMainRef, "expected at least one update to set main ref to new snapshot");
  }

  @Test
  void commitRemoveSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    long currentSnapshotId = FIXTURE.metadata().getCurrentSnapshotId();
    long refSnapshotId = FIXTURE.snapshots().get(0).getSnapshotId();

    Table existing =
        baseTable(tableId, nsId)
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("current-snapshot-id", Long.toString(currentSnapshotId))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(currentSnapshotId)
            .putFormatMetadata(
                "iceberg",
                FIXTURE.metadata().toBuilder()
                    .setCurrentSnapshotId(currentSnapshotId)
                    .putRefs(
                        "dev",
                        IcebergRef.newBuilder()
                            .setType("branch")
                            .setSnapshotId(refSnapshotId)
                            .build())
                    .build()
                    .toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(
            "{\"requirements\":[],\"updates\":[{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"dev\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub, atLeastOnce()).updateSnapshot(updateReq.capture());
    boolean removed =
        updateReq.getAllValues().stream()
            .map(req -> metadataFromSpec(req.getSpec()).getRefsMap())
            .anyMatch(refs -> !refs.containsKey("dev"));
    assertTrue(removed, "expected at least one update to remove the dev ref");
  }

  @Test
  void commitMetadataUpdatesModifyIcebergMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    long currentSnapshotId = FIXTURE.metadata().getCurrentSnapshotId();

    Table existing =
        baseTable(tableId, nsId)
            .putProperties("metadata-location", FIXTURE.metadataLocation())
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("current-snapshot-id", Long.toString(currentSnapshotId))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(currentSnapshotId)
            .putFormatMetadata(
                "iceberg",
                FIXTURE.metadata().toBuilder()
                    .clearSchemas()
                    .clearPartitionSpecs()
                    .clearSortOrders()
                    .clearStatistics()
                    .clearPartitionStatistics()
                    .clearEncryptionKeys()
                    .clearRefs()
                    .clearMetadataLog()
                    .clearSnapshotLog()
                    .setTableUuid("uuid")
                    .setFormatVersion(2)
                    .setCurrentSchemaId(1)
                    .setDefaultSpecId(1)
                    .setDefaultSortOrderId(1)
                    .addSchemas(
                        IcebergSchema.newBuilder()
                            .setSchemaId(1)
                            .setSchemaJson(
                                "{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"placeholder\",\"required\":false,\"type\":\"string\"}]}")
                            .build())
                    .addSchemas(
                        IcebergSchema.newBuilder()
                            .setSchemaId(3)
                            .setSchemaJson(
                                "{\"type\":\"struct\",\"fields\":[{\"id\":3,\"name\":\"c\",\"required\":false,\"type\":\"long\"}]}")
                            .build())
                    .addPartitionSpecs(
                        PartitionSpecInfo.newBuilder()
                            .setSpecId(1)
                            .setSpecName("spec-1")
                            .addFields(
                                PartitionField.newBuilder()
                                    .setFieldId(1)
                                    .setName("category")
                                    .setTransform("identity")
                                    .build())
                            .build())
                    .addPartitionSpecs(
                        PartitionSpecInfo.newBuilder()
                            .setSpecId(2)
                            .setSpecName("spec-2")
                            .addFields(
                                PartitionField.newBuilder()
                                    .setFieldId(2)
                                    .setName("region")
                                    .setTransform("identity")
                                    .build())
                            .build())
                    .addSortOrders(
                        IcebergSortOrder.newBuilder()
                            .setSortOrderId(1)
                            .addFields(
                                IcebergSortField.newBuilder()
                                    .setSourceFieldId(1)
                                    .setTransform("identity")
                                    .setDirection("ASC")
                                    .setNullOrder("nulls-first")
                                    .build())
                            .build())
                    .addStatistics(
                        IcebergStatisticsFile.newBuilder()
                            .setSnapshotId(3)
                            .setStatisticsPath("s3://stats/old.avro")
                            .setFileSizeInBytes(64)
                            .setFileFooterSizeInBytes(16)
                            .addBlobMetadata(
                                IcebergBlobMetadata.newBuilder()
                                    .setType("DATA")
                                    .setSnapshotId(3)
                                    .setSequenceNumber(2)
                                    .addFields(1)
                                    .putProperties("foo", "bar")
                                    .build())
                            .build())
                    .addPartitionStatistics(
                        IcebergPartitionStatisticsFile.newBuilder()
                            .setSnapshotId(2)
                            .setStatisticsPath("s3://stats/part_old.avro")
                            .setFileSizeInBytes(128)
                            .build())
                    .addEncryptionKeys(
                        IcebergEncryptedKey.newBuilder()
                            .setKeyId("old")
                            .setEncryptedKeyMetadata(ByteString.copyFromUtf8("old"))
                            .build())
                    .build()
                    .toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(
            """
            {"requirements":[],"updates":[
              {"action":"assign-uuid","uuid":"new-uuid"},
              {"action":"upgrade-format-version","format-version":3},
              {"action":"add-schema","schema":{
                "schema-id":7,
                "type":"struct",
                "fields":[{"id":7,"name":"new_col","required":false,"type":"string"}]
              }},
              {"action":"set-current-schema","schema-id":-1},
              {"action":"remove-schemas","schema-ids":[3]},
              {"action":"add-spec","spec":{
                "spec-id":5,
                "fields":[{"name":"category","source-id":1,"transform":"identity"}]
              }},
              {"action":"set-default-spec","spec-id":-1},
              {"action":"remove-partition-specs","spec-ids":[1]},
              {"action":"add-sort-order","sort-order":{
                "order-id":3,
                "fields":[{"source-id":1,"transform":"identity","direction":"ASC","null-order":"nulls-first"}]
              }},
              {"action":"set-default-sort-order","sort-order-id":-1},
              {"action":"set-statistics","statistics":{
                "snapshot-id":4,
                "statistics-path":"s3://stats/new.avro",
                "file-size-in-bytes":128,
                "file-footer-size-in-bytes":32,
                "blob-metadata":[{\"type\":\"DATA\",\"snapshot-id\":4,\"sequence-number\":10,\"fields\":[1,2],\"properties\":{\"bar\":\"baz\"}}]
              }},
              {"action":"remove-statistics","snapshot-id":3},
              {"action":"set-partition-statistics","partition-statistics":{
                "snapshot-id":4,
                "statistics-path":"s3://stats/partition_new.avro",
                "file-size-in-bytes":256
              }},
              {"action":"remove-partition-statistics","snapshot-id":2},
              {"action":"add-encryption-key","encryption-key":{
                "key-id":"new",
                "encrypted-key-metadata":"c2VjcmV0",
                "encrypted-by-id":"kms"
              }},
              {"action":"remove-encryption-key","key-id":"old"}
            ]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub, atLeastOnce()).updateSnapshot(updateReq.capture());
    IcebergMetadata updated =
        updateReq.getAllValues().stream()
            .map(req -> metadataFromSpec(req.getSpec()))
            .filter(meta -> "new-uuid".equals(meta.getTableUuid()))
            .findFirst()
            .orElse(null);
    assertTrue(updated != null, "expected updated metadata with new-uuid");
    assertEquals("new-uuid", updated.getTableUuid());
    assertEquals(3, updated.getFormatVersion());
    assertEquals(7, updated.getCurrentSchemaId());
    assertEquals(5, updated.getDefaultSpecId());
    assertEquals(3, updated.getDefaultSortOrderId());
    assertEquals(2, updated.getSchemasCount());
    assertEquals(1, updated.getSchemas(0).getSchemaId());
    assertEquals(7, updated.getSchemas(1).getSchemaId());
    assertEquals(2, updated.getPartitionSpecsCount());
    assertEquals(2, updated.getPartitionSpecs(0).getSpecId());
    assertEquals(5, updated.getPartitionSpecs(1).getSpecId());
    assertEquals(2, updated.getSortOrdersCount());
    assertEquals(3, updated.getSortOrders(1).getSortOrderId());
    assertEquals(1, updated.getStatisticsCount());
    assertEquals(4, updated.getStatistics(0).getSnapshotId());
    assertEquals("s3://stats/new.avro", updated.getStatistics(0).getStatisticsPath());
    assertEquals(1, updated.getPartitionStatisticsCount());
    assertEquals(4, updated.getPartitionStatistics(0).getSnapshotId());
    assertEquals(
        "s3://stats/partition_new.avro", updated.getPartitionStatistics(0).getStatisticsPath());
    assertEquals(1, updated.getEncryptionKeysCount());
    assertEquals("new", updated.getEncryptionKeys(0).getKeyId());
  }

  @Test
  void commitRequirementTableUuidMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table current =
        baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build())
            .putProperties("table-uuid", "actual-uuid")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body(
            "{\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"expected\"}],\"updates\":[]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitRequirementAssertRefSnapshotIdMismatchReturns409() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    long currentSnapshotId = FIXTURE.metadata().getCurrentSnapshotId();

    Table current =
        baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build())
            .putProperties("current-snapshot-id", Long.toString(currentSnapshotId))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setCurrentSnapshotId(currentSnapshotId)
            .putRefs(
                "main",
                IcebergRef.newBuilder().setSnapshotId(currentSnapshotId).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(currentSnapshotId)
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            String.format(
                """
                {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":%d}],
                 "updates":[{"action":"set-properties","updates":{"owner":"floecat"}}]}
                """,
                currentSnapshotId + 1))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(409)
        .body("error.type", equalTo("CommitFailedException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitRequirementAssertRefSnapshotIdAllowsUpdate() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    long currentSnapshotId = FIXTURE.metadata().getCurrentSnapshotId();

    Table current =
        baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build())
            .putProperties("current-snapshot-id", Long.toString(currentSnapshotId))
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setCurrentSnapshotId(currentSnapshotId)
            .putRefs(
                "main",
                IcebergRef.newBuilder().setSnapshotId(currentSnapshotId).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(currentSnapshotId)
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            String.format(
                """
                {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":%d}],
                 "updates":[{"action":"set-properties","updates":{"owner":"floecat"}}]}
                """,
                currentSnapshotId))
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    verify(tableStub, atLeastOnce()).updateTable(any());
  }

  @Test
  void commitUnknownRequirementReturns400() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table current = baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build()).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"requirements\":[{\"type\":\"unknown-requirement\"}],\"updates\":[]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).updateTable(any());
  }

  @Test
  void commitUnknownUpdateReturns400() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());
    Table current = baseTable(tableId, ResourceId.newBuilder().setId("cat:db").build()).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"updates\":[{\"action\":\"unknown-update\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(400)
        .body("error.type", equalTo("ValidationException"));

    verify(tableStub, never()).updateTable(any());
  }
}
