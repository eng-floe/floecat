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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedKey;
import org.junit.jupiter.api.Test;

class CanonicalTableMetadataServiceTest {

  @Test
  void toIcebergMetadataPersistsAvailableSupplementalMetadata() {
    CanonicalTableMetadataService service = new CanonicalTableMetadataService();
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
    CanonicalTableMetadataService service = new CanonicalTableMetadataService();
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
}
