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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

class IcebergMetadataServiceTest {

  @Test
  void toIcebergMetadataPersistsAvailableSupplementalMetadata() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    TableMetadata metadata = mock(TableMetadata.class);
    HistoryEntry snapshotLogEntry = mock(HistoryEntry.class);
    TableMetadata.MetadataLogEntry metadataLogEntry = mock(TableMetadata.MetadataLogEntry.class);
    StatisticsFile statisticsFile = mock(StatisticsFile.class);
    BlobMetadata blobMetadata = mock(BlobMetadata.class);
    PartitionStatisticsFile partitionStatisticsFile = mock(PartitionStatisticsFile.class);
    EncryptedKey encryptedKey = mock(EncryptedKey.class);

    when(metadata.uuid()).thenReturn("uuid-1");
    when(metadata.formatVersion()).thenReturn(2);
    when(metadata.lastUpdatedMillis()).thenReturn(123L);
    when(metadata.lastColumnId()).thenReturn(7);
    when(metadata.currentSchemaId()).thenReturn(3);
    when(metadata.defaultSpecId()).thenReturn(4);
    when(metadata.lastAssignedPartitionId()).thenReturn(5);
    when(metadata.defaultSortOrderId()).thenReturn(6);
    when(metadata.lastSequenceNumber()).thenReturn(8L);
    when(metadata.location()).thenReturn("s3://warehouse/table");
    when(metadata.nextRowId()).thenReturn(9L);
    when(metadata.properties()).thenReturn(Map.of("write.target-file-size-bytes", "268435456"));
    when(metadata.currentSnapshot()).thenReturn(null);
    when(metadata.snapshotLog()).thenReturn(List.of(snapshotLogEntry));
    when(metadata.previousFiles()).thenReturn(List.of(metadataLogEntry));
    when(metadata.schemas()).thenReturn(List.of());
    when(metadata.specs()).thenReturn(List.of());
    when(metadata.sortOrders()).thenReturn(List.of());
    when(metadata.refs()).thenReturn(Map.of());
    when(metadata.statisticsFiles()).thenReturn(List.of(statisticsFile));
    when(metadata.partitionStatisticsFiles()).thenReturn(List.of(partitionStatisticsFile));
    when(metadata.encryptionKeys()).thenReturn(List.of(encryptedKey));

    when(snapshotLogEntry.timestampMillis()).thenReturn(100L);
    when(snapshotLogEntry.snapshotId()).thenReturn(101L);

    when(metadataLogEntry.timestampMillis()).thenReturn(200L);
    when(metadataLogEntry.file()).thenReturn("s3://warehouse/metadata/00001.metadata.json");

    when(statisticsFile.snapshotId()).thenReturn(301L);
    when(statisticsFile.path()).thenReturn("s3://warehouse/stats.puffin");
    when(statisticsFile.fileSizeInBytes()).thenReturn(400L);
    when(statisticsFile.fileFooterSizeInBytes()).thenReturn(50L);
    when(statisticsFile.blobMetadata()).thenReturn(List.of(blobMetadata));

    when(blobMetadata.type()).thenReturn("apache-datasketches-theta-v1");
    when(blobMetadata.sourceSnapshotId()).thenReturn(302L);
    when(blobMetadata.sourceSnapshotSequenceNumber()).thenReturn(303L);
    when(blobMetadata.fields()).thenReturn(List.of(1, 2));
    when(blobMetadata.properties()).thenReturn(Map.of("k", "v"));

    when(partitionStatisticsFile.snapshotId()).thenReturn(401L);
    when(partitionStatisticsFile.path()).thenReturn("s3://warehouse/partition-stats.avro");
    when(partitionStatisticsFile.fileSizeInBytes()).thenReturn(402L);

    byte[] encrypted = new byte[] {1, 2, 3};
    when(encryptedKey.keyId()).thenReturn("key-1");
    when(encryptedKey.encryptedKeyMetadata()).thenReturn(ByteBuffer.wrap(encrypted));
    when(encryptedKey.encryptedById()).thenReturn("kms-1");

    IcebergMetadata icebergMetadata =
        service.toIcebergMetadata(metadata, "s3://warehouse/metadata/00002.metadata.json");

    assertEquals(1, icebergMetadata.getSnapshotLogCount());
    assertEquals(100L, icebergMetadata.getSnapshotLog(0).getTimestampMs());
    assertEquals(101L, icebergMetadata.getSnapshotLog(0).getSnapshotId());

    assertEquals(1, icebergMetadata.getMetadataLogCount());
    assertEquals(200L, icebergMetadata.getMetadataLog(0).getTimestampMs());
    assertEquals(
        "s3://warehouse/metadata/00001.metadata.json", icebergMetadata.getMetadataLog(0).getFile());

    assertEquals(1, icebergMetadata.getStatisticsCount());
    assertEquals(301L, icebergMetadata.getStatistics(0).getSnapshotId());
    assertEquals(
        "s3://warehouse/stats.puffin", icebergMetadata.getStatistics(0).getStatisticsPath());
    assertEquals(1, icebergMetadata.getStatistics(0).getBlobMetadataCount());
    assertEquals(
        "apache-datasketches-theta-v1",
        icebergMetadata.getStatistics(0).getBlobMetadata(0).getType());
    assertEquals(
        "v", icebergMetadata.getStatistics(0).getBlobMetadata(0).getPropertiesMap().get("k"));

    assertEquals(1, icebergMetadata.getPartitionStatisticsCount());
    assertEquals(401L, icebergMetadata.getPartitionStatistics(0).getSnapshotId());
    assertEquals(
        "s3://warehouse/partition-stats.avro",
        icebergMetadata.getPartitionStatistics(0).getStatisticsPath());

    assertEquals(1, icebergMetadata.getEncryptionKeysCount());
    assertEquals("key-1", icebergMetadata.getEncryptionKeys(0).getKeyId());
    assertEquals("kms-1", icebergMetadata.getEncryptionKeys(0).getEncryptedById());
    assertArrayEquals(
        encrypted, icebergMetadata.getEncryptionKeys(0).getEncryptedKeyMetadata().toByteArray());
    assertEquals(
        "268435456", icebergMetadata.getPropertiesMap().get("write.target-file-size-bytes"));
    assertEquals("s3://warehouse/table", icebergMetadata.getLocation());
    assertEquals(9L, icebergMetadata.getNextRowId());
  }

  @Test
  void toTableMetadataViewUsesNormalizedBuilderShape() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    TableMetadata metadata = mock(TableMetadata.class);
    org.apache.iceberg.Snapshot snapshot = mock(org.apache.iceberg.Snapshot.class);

    when(metadata.uuid()).thenReturn("uuid-2");
    when(metadata.formatVersion()).thenReturn(2);
    when(metadata.lastUpdatedMillis()).thenReturn(123L);
    when(metadata.lastColumnId()).thenReturn(1);
    when(metadata.currentSchemaId()).thenReturn(0);
    when(metadata.defaultSpecId()).thenReturn(0);
    when(metadata.lastAssignedPartitionId()).thenReturn(0);
    when(metadata.defaultSortOrderId()).thenReturn(0);
    when(metadata.lastSequenceNumber()).thenReturn(1L);
    when(metadata.location()).thenReturn("s3://warehouse/table");
    when(metadata.nextRowId()).thenReturn(2L);
    when(metadata.properties()).thenReturn(Map.of("owner", "alice"));
    when(metadata.currentSnapshot()).thenReturn(snapshot);
    when(metadata.snapshotLog()).thenReturn(List.of());
    when(metadata.previousFiles()).thenReturn(List.of());
    when(metadata.schemas()).thenReturn(List.of());
    when(metadata.specs()).thenReturn(List.of());
    when(metadata.sortOrders()).thenReturn(List.of());
    when(metadata.refs()).thenReturn(Map.of());
    when(metadata.statisticsFiles()).thenReturn(List.of());
    when(metadata.partitionStatisticsFiles()).thenReturn(List.of());
    when(metadata.encryptionKeys()).thenReturn(List.of());
    when(metadata.snapshots()).thenReturn(List.of(snapshot));

    when(snapshot.snapshotId()).thenReturn(11L);
    when(snapshot.parentId()).thenReturn(null);
    when(snapshot.timestampMillis()).thenReturn(456L);
    when(snapshot.sequenceNumber()).thenReturn(1L);
    when(snapshot.manifestListLocation()).thenReturn("s3://warehouse/metadata/snap-11.avro");
    when(snapshot.summary()).thenReturn(Map.of("operation", "append"));
    when(snapshot.schemaId()).thenReturn(0);

    TableMetadataView view =
        service.toTableMetadataView(metadata, "s3://warehouse/metadata/00003.metadata.json");

    assertEquals("uuid-2", view.tableUuid());
    assertEquals("s3://warehouse/table", view.location());
    assertEquals(11L, view.currentSnapshotId());
    assertEquals(1, view.snapshots().size());
    assertEquals("s3://warehouse/metadata/00003.metadata.json", view.metadataLocation());
    assertNotNull(view.properties());
    assertEquals("alice", view.properties().get("owner"));
  }

  @Test
  void bootstrapTableMetadataSynthesizesMetadataLocationFromTableLocation() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    Table table =
        Table.newBuilder()
            .putProperties("location", "s3://warehouse/orders")
            .putProperties("format-version", "2")
            .putProperties("current-schema-id", "0")
            .putProperties("last-column-id", "1")
            .putProperties("default-spec-id", "0")
            .putProperties("last-partition-id", "1000")
            .putProperties("default-sort-order-id", "0")
            .putProperties("table-uuid", "uuid-1")
            .setSchemaJson(
                "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}")
            .build();

    TableMetadata metadata =
        service.bootstrapTableMetadata("orders", table, table.getPropertiesMap(), null, List.of());

    assertNotNull(metadata);
    assertEquals("s3://warehouse/orders", metadata.location());
  }

  @Test
  void bootstrapTableMetadataReturnsNullWhenNoTableLocationExists() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    Table table =
        Table.newBuilder()
            .putProperties("format-version", "2")
            .putProperties("current-schema-id", "0")
            .putProperties("last-column-id", "1")
            .putProperties("default-spec-id", "0")
            .putProperties("default-sort-order-id", "0")
            .setSchemaJson(
                "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}")
            .build();

    TableMetadata metadata =
        service.bootstrapTableMetadata("orders", table, table.getPropertiesMap(), null, List.of());

    assertNull(metadata);
  }

  @Test
  void canonicalizeMaterializeResultUpdatesEmbeddedMetadataLocationProperty() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of(
                "metadata-location",
                "s3://warehouse/db/orders/metadata/00000-old.metadata.json",
                "owner",
                "alice"));

    IcebergMetadataService.MaterializeResult result =
        service.canonicalizeMaterializeResult(
            "s3://warehouse/db/orders/metadata/00001-new.metadata.json", metadata);

    assertEquals(
        "s3://warehouse/db/orders/metadata/00001-new.metadata.json", result.metadataLocation());
    assertEquals(
        "s3://warehouse/db/orders/metadata/00001-new.metadata.json",
        result.tableMetadata().metadataFileLocation());
    assertEquals(
        "s3://warehouse/db/orders/metadata/00001-new.metadata.json",
        result.tableMetadata().properties().get("metadata-location"));
    assertEquals("alice", result.tableMetadata().properties().get("owner"));
  }

  @Test
  void reserveCreateMetadataLocationUsesNextMetadataSequence() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    String root;
    try {
      root = Files.createTempDirectory("iceberg-meta-test").toString();
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
    Map<String, String> ioProps =
        Map.of("io-impl", InMemoryS3FileIO.class.getName(), "fs.floecat.test-root", root);
    FileIO fileIO = FileIoFactory.createFileIo(ioProps, null, false);
    try {
      TableMetadata metadata =
          TableMetadata.newTableMetadata(
              new org.apache.iceberg.Schema(
                  org.apache.iceberg.types.Types.NestedField.optional(
                      1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
              org.apache.iceberg.PartitionSpec.unpartitioned(),
              org.apache.iceberg.SortOrder.unsorted(),
              "s3://warehouse/db/orders",
              Map.of("table-uuid", "uuid-1"));
      TableMetadataParser.write(
          metadata,
          fileIO.newOutputFile("s3://warehouse/db/orders/metadata/00000-old.metadata.json"));
      TableMetadataParser.write(
          metadata,
          fileIO.newOutputFile("s3://warehouse/db/orders/metadata/00001-old.metadata.json"));

      String reserved =
          service.reserveCreateMetadataLocation("s3://warehouse/db/orders", "uuid-2", ioProps);

      assertEquals("s3://warehouse/db/orders/metadata/00002-uuid-2.metadata.json", reserved);
    } finally {
      FileIoFactory.closeQuietly(fileIO);
    }
  }

  @Test
  void reserveCreateMetadataLocationRejectsMissingInputs() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.reserveCreateMetadataLocation(null, "uuid-1", Map.of()));

    assertTrue(error.getMessage().contains("reserve metadata location"));
  }

  @Test
  void materializeHonorsExactMetadataLocationOverride() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());
    service.tableGatewaySupport = mock(TableGatewaySupport.class);
    when(service.tableGatewaySupport.defaultFileIoProperties())
        .thenReturn(Map.of("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO"));

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of("table-uuid", "uuid-1"));

    IcebergMetadataService.MaterializeResult result =
        service.materializeAtExactLocation(
            "db",
            "orders",
            metadata,
            "s3://warehouse/db/orders/metadata/00003-reserved.metadata.json");

    assertEquals(
        "s3://warehouse/db/orders/metadata/00003-reserved.metadata.json",
        result.metadataLocation());
    assertEquals(
        "s3://warehouse/db/orders/metadata/00003-reserved.metadata.json",
        result.tableMetadata().metadataFileLocation());
  }

  @Test
  void materializeWithoutExplicitOverrideCreatesNextMetadataFile() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    String root;
    try {
      root = Files.createTempDirectory("iceberg-meta-versioned-test").toString();
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
    service.tableGatewaySupport = mock(TableGatewaySupport.class);
    when(service.tableGatewaySupport.defaultFileIoProperties())
        .thenReturn(
            Map.of("io-impl", InMemoryS3FileIO.class.getName(), "fs.floecat.test-root", root));

    Map<String, String> ioProps =
        Map.of("io-impl", InMemoryS3FileIO.class.getName(), "fs.floecat.test-root", root);
    FileIO fileIO = FileIoFactory.createFileIo(ioProps, null, false);
    try {
      TableMetadata existing =
          TableMetadata.newTableMetadata(
              new org.apache.iceberg.Schema(
                  org.apache.iceberg.types.Types.NestedField.optional(
                      1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
              org.apache.iceberg.PartitionSpec.unpartitioned(),
              org.apache.iceberg.SortOrder.unsorted(),
              "s3://warehouse/db/orders",
              Map.of("table-uuid", "uuid-1"));
      TableMetadataParser.write(
          existing,
          fileIO.newOutputFile("s3://warehouse/db/orders/metadata/00000-old.metadata.json"));

      TableMetadata next =
          TableMetadata.buildFrom(existing)
              .discardChanges()
              .withMetadataLocation("s3://warehouse/db/orders/metadata/00000-old.metadata.json")
              .build();

      IcebergMetadataService.MaterializeResult result =
          service.materializeNextVersion("db", "orders", next);

      assertNotNull(result.metadataLocation());
      assertTrue(result.metadataLocation().startsWith("s3://warehouse/db/orders/metadata/00001-"));
      assertTrue(result.metadataLocation().endsWith(".metadata.json"));
      assertEquals(result.metadataLocation(), result.tableMetadata().metadataFileLocation());
    } finally {
      FileIoFactory.closeQuietly(fileIO);
    }
  }

  @Test
  void applyCommitUpdatesFailsForUnknownSnapshotRefTarget() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of());
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "branch",
                    "snapshot-id",
                    999L,
                    "type",
                    "branch")));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.applyCommitUpdates(metadata, Table.newBuilder().build(), request));

    assertEquals(
        "set-snapshot-ref failed for ref branch: unknown snapshot 999", error.getMessage());
  }

  @Test
  void applyCommitUpdatesProcessesRefMutationAfterDuplicateAddSnapshot() {
    IcebergMetadataService service = new IcebergMetadataService();
    service.setMapper(new ObjectMapper());

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(
                    1, "id", org.apache.iceberg.types.Types.IntegerType.get())),
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of());

    Map<String, Object> snapshot =
        new LinkedHashMap<>(
            Map.of(
                "snapshot-id",
                7L,
                "timestamp-ms",
                1000L,
                "manifest-list",
                "s3://warehouse/db/orders/metadata/manifest.avro",
                "summary",
                Map.of("operation", "append")));
    Map<String, Object> addSnapshot = new LinkedHashMap<>();
    addSnapshot.put("action", "add-snapshot");
    addSnapshot.put("snapshot", snapshot);

    Map<String, Object> duplicateAddSnapshot = new LinkedHashMap<>();
    duplicateAddSnapshot.put("action", "add-snapshot");
    duplicateAddSnapshot.put("snapshot", new LinkedHashMap<>(snapshot));

    Map<String, Object> setSnapshotRef = new LinkedHashMap<>();
    setSnapshotRef.put("action", "set-snapshot-ref");
    setSnapshotRef.put("ref-name", "branch");
    setSnapshotRef.put("snapshot-id", 7L);
    setSnapshotRef.put("type", "branch");

    TableMetadata updated =
        service.applyCommitUpdates(
            metadata,
            Table.newBuilder().build(),
            new TableRequests.Commit(
                List.of(), List.of(addSnapshot, duplicateAddSnapshot, setSnapshotRef)));

    assertNotNull(updated.refs().get("branch"));
    assertEquals(7L, updated.refs().get("branch").snapshotId());
  }
}
