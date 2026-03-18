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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IcebergMetadataCommitServiceTest {
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final TableMetadataImportService importService = new TableMetadataImportService(config);
  private final IcebergMetadataCommitService service =
      new IcebergMetadataCommitService(importService, config, new ObjectMapper());

  @Test
  void bootstrapsInitialMetadataWhenTableHasNoMetadataLocation() {
    when(config.metadataFileIo())
        .thenReturn(Optional.of("org.apache.iceberg.inmemory.InMemoryFileIO"));
    when(config.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(config.metadataS3Endpoint()).thenReturn(Optional.empty());
    when(config.metadataS3Region()).thenReturn(Optional.empty());
    when(config.metadataClientRegion()).thenReturn(Optional.empty());
    when(config.metadataS3AccessKeyId()).thenReturn(Optional.empty());
    when(config.metadataS3SecretAccessKey()).thenReturn(Optional.empty());
    when(config.metadataS3PathStyleAccess()).thenReturn(true);

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("table-uuid", "uuid-123")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "sequence-number",
                        0L,
                        "timestamp-ms",
                        123456789L,
                        "manifest-list",
                        "s3://warehouse/db/orders/metadata/snap-101.avro",
                        "summary",
                        Map.of("operation", "append"))),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    101L,
                    "type",
                    "branch"),
                Map.of(
                    "action",
                    "set-statistics",
                    "statistics",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "statistics-path",
                        "s3://warehouse/db/orders/stats/101.puffin"))));

    String metadataLocation = planned.table().getPropertiesOrThrow("metadata-location");
    assertNotNull(metadataLocation);
    assertFalse(metadataLocation.isBlank());
    assertEquals("101", planned.table().getPropertiesOrThrow("current-snapshot-id"));
    assertEquals("0", planned.table().getPropertiesOrThrow("last-sequence-number"));
    assertNotNull(planned.table().getPropertiesMap().get("table-uuid"));
    assertFalse(planned.table().getPropertiesOrThrow("table-uuid").isBlank());
  }

  @Test
  void bootstrapsInitialMetadataAtRequestedFormatVersion() {
    when(config.metadataFileIo())
        .thenReturn(Optional.of("org.apache.iceberg.inmemory.InMemoryFileIO"));
    when(config.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(config.metadataS3Endpoint()).thenReturn(Optional.empty());
    when(config.metadataS3Region()).thenReturn(Optional.empty());
    when(config.metadataClientRegion()).thenReturn(Optional.empty());
    when(config.metadataS3AccessKeyId()).thenReturn(Optional.empty());
    when(config.metadataS3SecretAccessKey()).thenReturn(Optional.empty());
    when(config.metadataS3PathStyleAccess()).thenReturn(true);

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson(
                "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"}]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "1")
            .putProperties("table-uuid", "uuid-123")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "sequence-number",
                        0L,
                        "timestamp-ms",
                        123456789L,
                        "manifest-list",
                        "s3://warehouse/db/orders/metadata/snap-101.avro",
                        "summary",
                        Map.of("operation", "append"))),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    101L,
                    "type",
                    "branch")));

    TableMetadata metadata =
        importService.loadTableMetadata(
            planned.table().getPropertiesOrThrow("metadata-location"), Map.of());

    assertEquals(1, metadata.formatVersion());
  }

  @Test
  void rebuildsEmptyBootstrappedMetadataAtRequestedFormatVersion() {
    stubInMemoryFileIo();
    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson(
                "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"}]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("table-uuid", "uuid-123")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(),
            List.of(
                Map.of("action", "upgrade-format-version", "format-version", 1),
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of(
                        "type",
                        "struct",
                        "schema-id",
                        0,
                        "fields",
                        List.of(Map.of("id", 1, "name", "id", "required", false, "type", "int")))),
                Map.of("action", "set-current-schema", "schema-id", 0)));

    TableMetadata metadata =
        importService.loadTableMetadata(
            planned.table().getPropertiesOrThrow("metadata-location"), Map.of());

    assertEquals(1, metadata.formatVersion());
  }

  @Test
  void rejectsRemoveSnapshotsWithoutIds() {
    stubInMemoryFileIo();
    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.plan(
                    currentTable,
                    ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
                    List.of(),
                    List.of(Map.of("action", "remove-snapshots", "snapshot-ids", List.of()))));

    assertTrue(
        error.getStatus().getDescription().contains("remove-snapshots requires snapshot-ids"));
  }

  @Test
  void appliesRemoveSpecRemoveSchemaAndEncryptionUpdates() throws Exception {
    stubInMemoryFileIo();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String metadataLocation = uniqueMetadataLocation("orders");
    Schema schema0 = new Schema();
    Schema schema1 =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
    PartitionSpec spec1 = PartitionSpec.builderFor(schema1).withSpecId(1).identity("id").build();

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
                schema0,
                PartitionSpec.unpartitioned(),
                org.apache.iceberg.SortOrder.unsorted(),
                "s3://warehouse/db/orders",
                Map.of())
            .updateSchema(schema1)
            .updatePartitionSpec(spec1);
    metadata =
        TableMetadata.buildFrom(metadata)
            .addEncryptionKey(
                new BaseEncryptedKey(
                    "key-0", ByteBuffer.wrap(Base64.getDecoder().decode("AQID")), null, Map.of()))
            .build();

    FileIO fileIO = new org.apache.iceberg.inmemory.InMemoryFileIO();
    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .putAllProperties(importService.canonicalProperties(metadata, metadataLocation))
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            tableId,
            List.of(),
            List.of(
                Map.of("action", "remove-partition-specs", "spec-ids", List.of(0)),
                Map.of("action", "remove-schemas", "schema-ids", List.of(0)),
                Map.of(
                    "action",
                    "add-encryption-key",
                    "encryption-key",
                    Map.of(
                        "key-id", "key-1",
                        "encrypted-key-metadata", "BAUG")),
                Map.of("action", "remove-encryption-key", "key-id", "key-0")));

    TableMetadata updated =
        importService.loadTableMetadata(
            planned.table().getPropertiesOrThrow("metadata-location"), Map.of());

    assertEquals(List.of(1), updated.schemas().stream().map(Schema::schemaId).toList());
    assertEquals(List.of(1), updated.specs().stream().map(PartitionSpec::specId).toList());
    assertEquals(
        List.of("key-1"), updated.encryptionKeys().stream().map(key -> key.keyId()).toList());
  }

  @Test
  void rejectsMalformedEncryptionUpdate() {
    stubInMemoryFileIo();
    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.planCreate(
                    currentTable,
                    ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
                    List.of(
                        Map.of(
                            "action",
                            "add-schema",
                            "schema",
                            Map.of("type", "struct", "schema-id", 0, "fields", List.of())),
                        Map.of("action", "set-location", "location", "s3://warehouse/db/orders"),
                        Map.of(
                            "action",
                            "add-encryption-key",
                            "encryption-key",
                            Map.of("key-id", "key-1", "encrypted-key-metadata", "not-base64")))));

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }

  @Test
  void createPlanKeepsLocationEmptyForInitialEngineMaterialization() {
    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.planCreate(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of("type", "struct", "schema-id", 0, "fields", List.of())),
                Map.of("action", "set-current-schema", "schema-id", 0),
                Map.of("action", "set-location", "location", "s3://warehouse/db/orders"),
                Map.of("action", "upgrade-format-version", "format-version", 2)));

    assertFalse(planned.table().getPropertiesMap().containsKey("metadata-location"));
    assertEquals("s3://warehouse/db/orders", planned.table().getPropertiesOrThrow("location"));
    assertEquals("2", planned.table().getPropertiesOrThrow("format-version"));
  }

  @Test
  void createPlanMaterializesMetadataWhenCreateCarriesSnapshotUpdates() {
    stubInMemoryFileIo();
    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("table-uuid", "uuid-123")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.planCreate(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of(
                        "type",
                        "struct",
                        "schema-id",
                        0,
                        "fields",
                        List.of(Map.of("id", 1, "name", "id", "required", false, "type", "int")))),
                Map.of("action", "set-current-schema", "schema-id", 0),
                Map.of("action", "set-location", "location", "s3://warehouse/db/orders"),
                Map.of("action", "upgrade-format-version", "format-version", 2),
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "sequence-number",
                        1L,
                        "timestamp-ms",
                        123456789L,
                        "manifest-list",
                        "s3://warehouse/db/orders/metadata/snap-101.avro",
                        "summary",
                        Map.of("operation", "append"),
                        "schema-id",
                        0)),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    101L,
                    "type",
                    "branch")));

    String metadataLocation = planned.table().getPropertiesOrThrow("metadata-location");
    assertNotNull(metadataLocation);
    assertTrue(metadataLocation.contains("/metadata/"));
    assertEquals("101", planned.table().getPropertiesOrThrow("current-snapshot-id"));
    assertEquals("1", planned.table().getPropertiesOrThrow("last-sequence-number"));
    assertNotNull(planned.table().getPropertiesMap().get("table-uuid"));
    assertFalse(planned.table().getPropertiesOrThrow("table-uuid").isBlank());
    assertEquals(2, planned.extraChanges().size());

    TableMetadata updated = importService.loadTableMetadata(metadataLocation, Map.of());
    assertNotNull(updated.currentSnapshot());
    assertEquals(101L, updated.currentSnapshot().snapshotId());
  }

  @Test
  void assertRefSnapshotIdAllowsMissingSnapshotIdForCompatibility() {
    stubInMemoryFileIo();
    String metadataLocation = uniqueMetadataLocation("orders");
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of());
    FileIO fileIO = new org.apache.iceberg.inmemory.InMemoryFileIO();
    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .putAllProperties(importService.canonicalProperties(metadata, metadataLocation))
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main")),
            List.of());

    assertEquals("orders", planned.table().getDisplayName());
    assertTrue(planned.extraChanges().isEmpty());
  }

  @Test
  void assertRefSnapshotIdSkipsStrictCheckWhenRefCannotBeResolved() {
    stubInMemoryFileIo();
    String metadataLocation = uniqueMetadataLocation("orders");
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of());
    FileIO fileIO = new org.apache.iceberg.inmemory.InMemoryFileIO();
    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .putAllProperties(importService.canonicalProperties(metadata, metadataLocation))
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main", "snapshot-id", 101L)),
            List.of());

    assertEquals("orders", planned.table().getDisplayName());
    assertTrue(planned.extraChanges().isEmpty());
  }

  private void stubInMemoryFileIo() {
    when(config.metadataFileIo())
        .thenReturn(Optional.of("org.apache.iceberg.inmemory.InMemoryFileIO"));
    when(config.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(config.metadataS3Endpoint()).thenReturn(Optional.empty());
    when(config.metadataS3Region()).thenReturn(Optional.empty());
    when(config.metadataClientRegion()).thenReturn(Optional.empty());
    when(config.metadataS3AccessKeyId()).thenReturn(Optional.empty());
    when(config.metadataS3SecretAccessKey()).thenReturn(Optional.empty());
    when(config.metadataS3PathStyleAccess()).thenReturn(true);
  }

  private String uniqueMetadataLocation(String tableName) {
    return "s3://warehouse/db/" + tableName + "/metadata/" + UUID.randomUUID() + ".metadata.json";
  }
}
