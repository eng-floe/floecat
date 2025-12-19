package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(RestResourceTestProfile.class)
class TableCommitResourceTest extends AbstractRestResourceTest {

  @Test
  void commitSupportsSetLocationUpdate() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setUri("s3://bucket/path/")
            .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
            .build();
    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    UpstreamRef updatedUpstream = upstream.toBuilder().setUri("s3://bucket/new_path/").build();
    Table updated = Table.newBuilder(current).setUpstream(updatedUpstream).build();
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(updated).build());

    given()
        .body(
            "{\"updates\":[{\"action\":\"set-location\",\"location\":\"s3://bucket/new_path/\"}]}")
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

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties(
                "metadata-location", "s3://bucket/orders/metadata/00000-abc.metadata.json")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .setUpstream(upstream)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    given()
        .body(
            "{\"updates\":[{\"action\":\"add-snapshot\",\"snapshot\":{"
                + "\"snapshot-id\":5,"
                + "\"timestamp-ms\":1000,"
                + "\"parent-snapshot-id\":4,"
                + "\"manifest-list\":\"s3://bucket/manifest.avro\","
                + "\"summary\":{\"operation\":\"append\"}"
                + "}}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<CreateSnapshotRequest> snapReq =
        ArgumentCaptor.forClass(CreateSnapshotRequest.class);
    verify(snapshotStub).createSnapshot(snapReq.capture());
    assertEquals(5, snapReq.getValue().getSpec().getSnapshotId());
    verify(connectorsStub).triggerReconcile(any());
    verify(connectorsStub).syncCapture(any());
  }

  @Test
  void commitRemoveSnapshotsCallsDelete() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    given()
        .body("{\"updates\":[{\"action\":\"remove-snapshots\",\"snapshot-ids\":[7,8]}]}")
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
    verify(connectorsStub, never()).syncCapture(any());
  }

  @Test
  void commitSetSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    UpstreamRef upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(ResourceId.newBuilder().setId("conn").build())
            .build();
    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .setUpstream(upstream)
            .putProperties(
                "metadata-location", "s3://bucket/orders/metadata/00000-abc.metadata.json")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("current-snapshot-id", "4")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5)
            .putFormatMetadata(
                "iceberg",
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .putRefs(
                        "main", IcebergRef.newBuilder().setType("branch").setSnapshotId(4).build())
                    .build()
                    .toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body(
            """
            {"updates":[
              {"action":"add-snapshot","snapshot":{
                "snapshot-id":5,
                "timestamp-ms":1000,
                "manifest-list":"s3://bucket/manifest.avro",
                "summary":{"operation":"append"}
              }},
              {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":5}
            ]}
            """)
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    assertEquals(5L, updateReq.getValue().getSpec().getSnapshotId());
    assertEquals(
        5L,
        metadataFromSpec(updateReq.getValue().getSpec()).getRefsOrThrow("main").getSnapshotId());
  }

  @Test
  void commitRemoveSnapshotRefUpdatesMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("current-snapshot-id", "9")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(9)
            .putFormatMetadata(
                "iceberg",
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .putRefs(
                        "dev", IcebergRef.newBuilder().setType("branch").setSnapshotId(8).build())
                    .build()
                    .toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build());

    given()
        .body("{\"updates\":[{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"dev\"}]}")
        .header("Content-Type", "application/json")
        .when()
        .post("/v1/foo/namespaces/db/tables/orders")
        .then()
        .statusCode(200);

    ArgumentCaptor<UpdateSnapshotRequest> updateReq =
        ArgumentCaptor.forClass(UpdateSnapshotRequest.class);
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    assertFalse(metadataFromSpec(updateReq.getValue().getSpec()).getRefsMap().containsKey("dev"));
  }

  @Test
  void commitMetadataUpdatesModifyIcebergMetadata() {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    Table existing =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setId("cat"))
            .setNamespaceId(nsId)
            .putProperties("current-snapshot-id", "4")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(existing).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(existing).build());

    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(4)
            .putFormatMetadata(
                "iceberg",
                IcebergMetadata.newBuilder()
                    .setTableUuid("uuid")
                    .setFormatVersion(2)
                    .setCurrentSchemaId(1)
                    .setDefaultSpecId(1)
                    .setDefaultSortOrderId(1)
                    .addSchemas(
                        IcebergSchema.newBuilder()
                            .setSchemaId(1)
                            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                            .build())
                    .addSchemas(
                        IcebergSchema.newBuilder()
                            .setSchemaId(3)
                            .setSchemaJson(
                                "{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"long\"}]}")
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
            {"updates":[
              {"action":"assign-uuid","uuid":"new-uuid"},
              {"action":"upgrade-format-version","format-version":3},
              {"action":"add-schema","schema":{
                "schema-id":7,
                "type":"struct",
                "fields":[]
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
    verify(snapshotStub).updateSnapshot(updateReq.capture());
    IcebergMetadata updated = metadataFromSpec(updateReq.getValue().getSpec());
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
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("table-uuid", "actual-uuid")
            .build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"requirements\":[{\"type\":\"assert-table-uuid\",\"uuid\":\"expected\"}]}")
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

    Table current =
        Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "5").build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(5).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5)
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            """
            {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":7}],
             "updates":[{"action":"set-properties","updates":{"owner":"floecat"}}]}
            """)
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

    Table current =
        Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "5").build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(current).build());

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(5).setType("branch").build())
            .build();
    Snapshot metaSnapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5)
            .putFormatMetadata("iceberg", metadata.toByteString())
            .build();
    when(snapshotStub.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(metaSnapshot).build());

    given()
        .body(
            """
            {"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":5}],
             "updates":[{"action":"set-properties","updates":{"owner":"floecat"}}]}
            """)
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
    Table current = Table.newBuilder().setResourceId(tableId).build();
    when(tableStub.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(current).build());

    given()
        .body("{\"requirements\":[{\"type\":\"unknown-requirement\"}]}")
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
    Table current = Table.newBuilder().setResourceId(tableId).build();
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
