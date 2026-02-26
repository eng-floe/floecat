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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import io.delta.kernel.engine.Engine;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeltaManifestMaterializerTest {

  private final QueryClient queryClient = mock(QueryClient.class);
  private final QuerySchemaClient querySchemaClient = mock(QuerySchemaClient.class);
  private final TestDeltaManifestMaterializer materializer = new TestDeltaManifestMaterializer();

  @BeforeEach
  void setUp() {
    materializer.queryClient = queryClient;
    materializer.querySchemaClient = querySchemaClient;

    when(queryClient.beginQuery(any()))
        .thenReturn(
            BeginQueryResponse.newBuilder()
                .setQuery(QueryDescriptor.newBuilder().setQueryId("q-1").build())
                .build());
    when(querySchemaClient.describeInputs(any()))
        .thenReturn(DescribeInputsResponse.newBuilder().build());

    ScanFile scanFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/part-00000.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(256)
            .setRecordCount(42)
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(ScanBundle.newBuilder().addDataFiles(scanFile).build())
                .build());
  }

  @Test
  void materializeReusesCompatManifestListWhenSnapshotUnchanged() {
    Table table = deltaTable("reuse");
    Snapshot snapshot = snapshot(7L, 7L);

    List<Snapshot> first = materializer.materialize(table, List.of(snapshot));
    String firstManifestList = first.get(0).getManifestList();
    assertFalse(firstManifestList.isBlank());
    assertTrue(materializer.fileIo().newInputFile(firstManifestList).exists());

    List<Snapshot> second = materializer.materialize(table, List.of(snapshot));
    String secondManifestList = second.get(0).getManifestList();
    assertEquals(firstManifestList, secondManifestList);

    verify(queryClient, times(1)).fetchScanBundle(any());
  }

  @Test
  void materializeRegeneratesCompatManifestListWhenSnapshotChanges() {
    Table table = deltaTable("regenerate");
    Snapshot oldSnapshot = snapshot(7L, 7L);
    Snapshot newSnapshot = snapshot(8L, 8L);

    String oldManifestList =
        materializer.materialize(table, List.of(oldSnapshot)).get(0).getManifestList();
    String newManifestList =
        materializer.materialize(table, List.of(newSnapshot)).get(0).getManifestList();

    assertFalse(oldManifestList.isBlank());
    assertFalse(newManifestList.isBlank());
    assertNotEquals(oldManifestList, newManifestList);
    assertTrue(materializer.fileIo().newInputFile(newManifestList).exists());

    verify(queryClient, times(2)).fetchScanBundle(any());
  }

  @Test
  void materializePopulatesManifestListForAllReturnedSnapshots() {
    Table table = deltaTable("all-snapshots");
    Snapshot firstSnapshot = snapshot(7L, 7L);
    Snapshot secondSnapshot = snapshot(8L, 8L);

    List<Snapshot> first = materializer.materialize(table, List.of(firstSnapshot, secondSnapshot));
    String firstManifestList = first.get(0).getManifestList();
    String secondManifestList = first.get(1).getManifestList();
    assertFalse(firstManifestList.isBlank());
    assertFalse(secondManifestList.isBlank());
    assertTrue(materializer.fileIo().newInputFile(firstManifestList).exists());
    assertTrue(materializer.fileIo().newInputFile(secondManifestList).exists());

    List<Snapshot> second = materializer.materialize(table, List.of(firstSnapshot, secondSnapshot));
    assertEquals(firstManifestList, second.get(0).getManifestList());
    assertEquals(secondManifestList, second.get(1).getManifestList());

    verify(queryClient, times(2)).fetchScanBundle(any());
  }

  @Test
  void materializeWritesDeleteManifestWhenDeleteFilesPresent() throws Exception {
    TestDeltaManifestMaterializerWithDeletes withDeletes =
        new TestDeltaManifestMaterializerWithDeletes();
    withDeletes.queryClient = queryClient;
    withDeletes.querySchemaClient = querySchemaClient;

    Table table = deltaTable("with-deletes");
    Snapshot snapshot = snapshot(9L, 9L);

    List<Snapshot> out = withDeletes.materialize(table, List.of(snapshot));
    String manifestList = out.get(0).getManifestList();
    assertFalse(manifestList.isBlank());
    ManifestFile deleteManifest =
        readManifestList(withDeletes.fileIo().newInputFile(manifestList)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));
    assertTrue(withDeletes.fileIo().newInputFile(deleteManifest.path()).exists());
  }

  @Test
  void materializePersistsColumnStatsAsIcebergMetrics() throws Exception {
    ScanFile scanFileWithStats =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/part-00001.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(512)
            .setRecordCount(50)
            .addColumns(
                ColumnStats.newBuilder()
                    .setColumnId(1)
                    .setLogicalType("INT")
                    .setValueCount(50)
                    .setNullCount(2)
                    .setMin("10")
                    .setMax("200")
                    .build())
            .addColumns(
                ColumnStats.newBuilder()
                    .setColumnId(2)
                    .setLogicalType("STRING")
                    .setValueCount(50)
                    .setMin("alpha")
                    .setMax("omega")
                    .build())
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(ScanBundle.newBuilder().addDataFiles(scanFileWithStats).build())
                .build());

    Table table = deltaTable("column-metrics");
    Snapshot snapshot = snapshot(11L, 11L);
    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    DataFile dataFile = readSingleDataFileFromManifestList(materializer.fileIo(), manifestListPath);
    assertEquals(50L, dataFile.valueCounts().get(1));
    assertEquals(2L, dataFile.nullValueCounts().get(1));
    assertEquals(
        10L,
        (Long) Conversions.fromByteBuffer(Types.LongType.get(), dataFile.lowerBounds().get(1)));
    assertEquals(
        200L,
        (Long) Conversions.fromByteBuffer(Types.LongType.get(), dataFile.upperBounds().get(1)));
    assertEquals(
        "alpha",
        Conversions.fromByteBuffer(Types.StringType.get(), dataFile.lowerBounds().get(2))
            .toString());
    assertEquals(
        "omega",
        Conversions.fromByteBuffer(Types.StringType.get(), dataFile.upperBounds().get(2))
            .toString());
  }

  @Test
  void materializePropagatesPartitionSpecAndValuesToDataManifest() throws Exception {
    String schemaJson =
        "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"region\",\"type\":\"string\",\"required\":false}],\"last-column-id\":1}";
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(12L)
            .setSequenceNumber(12L)
            .setSchemaJson(schemaJson)
            .setPartitionSpec(
                PartitionSpecInfo.newBuilder()
                    .setSpecId(7)
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(1)
                            .setName("region")
                            .setTransform("identity")
                            .build())
                    .build())
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .setSchemaJson(schemaJson)
            .putProperties("storage_location", "s3://floecat-delta/call_center/partitioned")
            .build();
    ScanFile partitionedFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/part-00002.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(1024)
            .setRecordCount(10)
            .setPartitionSpecId(7)
            .setPartitionDataJson(
                "{\"partitionValues\":[{\"id\":\"region\",\"value\":\"us-east-1\"}]}")
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(ScanBundle.newBuilder().addDataFiles(partitionedFile).build())
                .build());

    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    ManifestFile dataManifest =
        readManifestList(materializer.fileIo().newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DATA)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Data manifest not found in manifest list"));
    assertEquals(0, dataManifest.partitionSpecId());
    DataFile dataFile = readSingleDataFileFromManifest(materializer.fileIo(), dataManifest);
    assertEquals(0, dataFile.specId());
    assertEquals("us-east-1", dataFile.partition().get(0, String.class));
  }

  @Test
  void materializeResolvesPartitionIdsFromSourceNameAndNumericIds() throws Exception {
    String schemaJson =
        "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"region\",\"type\":\"string\",\"required\":false},{\"id\":2,\"name\":\"customer_id\",\"type\":\"int\",\"required\":false}],\"last-column-id\":2}";
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(17L)
            .setSequenceNumber(17L)
            .setSchemaJson(schemaJson)
            .setPartitionSpec(
                PartitionSpecInfo.newBuilder()
                    .setSpecId(8)
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(1)
                            .setName("region_part")
                            .setTransform("identity")
                            .build())
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(2)
                            .setName("cid_bucket")
                            .setTransform("bucket[8]")
                            .build())
                    .build())
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .setSchemaJson(schemaJson)
            .putProperties(
                "storage_location", "s3://floecat-delta/call_center/partition-id-encodings")
            .build();
    ScanFile dataFileWithMixedIds =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/part-mixed-ids.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(222)
            .setRecordCount(9)
            .setPartitionSpecId(8)
            .setPartitionDataJson(
                "{\"partitionValues\":[{\"id\":\"region\",\"value\":\"us-west-2\"},{\"id\":\"2\",\"value\":5}]}")
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(ScanBundle.newBuilder().addDataFiles(dataFileWithMixedIds).build())
                .build());

    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());
    DataFile dataFile = readSingleDataFileFromManifestList(materializer.fileIo(), manifestListPath);
    assertEquals("us-west-2", dataFile.partition().get(0, String.class));
    assertEquals(5, dataFile.partition().get(1, Integer.class));
  }

  @Test
  void materializeBuildsDeleteManifestFromScanBundleDeleteFiles() throws Exception {
    ScanFile dataFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/data-000.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(100)
            .setRecordCount(5)
            .addDeleteFileIndices(0)
            .build();
    ScanFile positionDelete =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/deletes-pos-000.avro")
            .setFileFormat("AVRO")
            .setFileContent(
                ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES)
            .setFileSizeInBytes(10)
            .setRecordCount(2)
            .build();
    ScanFile equalityDelete =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/deletes-eq-000.parquet")
            .setFileFormat("PARQUET")
            .setFileContent(
                ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES)
            .addEqualityFieldIds(2)
            .addEqualityFieldIds(3)
            .setFileSizeInBytes(20)
            .setRecordCount(3)
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(dataFile)
                        .addDeleteFiles(positionDelete)
                        .addDeleteFiles(equalityDelete)
                        .build())
                .build());

    Table table = deltaTable("bundle-deletes");
    Snapshot snapshot = snapshot(13L, 13L);
    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    ManifestFile deleteManifest =
        readManifestList(materializer.fileIo().newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));
    List<DeleteFile> deleteFiles =
        readDeleteFilesFromManifest(
            materializer.fileIo(),
            deleteManifest,
            Map.of(deleteManifest.partitionSpecId(), PartitionSpec.unpartitioned()));
    assertEquals(2, deleteFiles.size());

    DeleteFile posDelete =
        deleteFiles.stream()
            .filter(df -> df.content() == FileContent.POSITION_DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Position delete file missing"));
    assertEquals("s3://floecat-delta/call_center/data-000.parquet", posDelete.referencedDataFile());

    DeleteFile eqDelete =
        deleteFiles.stream()
            .filter(df -> df.content() == FileContent.EQUALITY_DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Equality delete file missing"));
    assertEquals(List.of(2, 3), eqDelete.equalityFieldIds());
  }

  @Test
  void materializePersistsDataAndDeleteSequenceNumbersInManifestEntries() throws Exception {
    ScanFile dataFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/data-seq.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(100)
            .setRecordCount(5)
            .setSequenceNumber(101)
            .addDeleteFileIndices(0)
            .build();
    ScanFile positionDelete =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/deletes-seq.avro")
            .setFileFormat("AVRO")
            .setFileContent(
                ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES)
            .setFileSizeInBytes(10)
            .setRecordCount(2)
            .setSequenceNumber(202)
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(dataFile)
                        .addDeleteFiles(positionDelete)
                        .build())
                .build());

    Table table = deltaTable("manifest-sequences");
    Snapshot snapshot = snapshot(18L, 18L);
    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    List<ManifestFile> manifests =
        readManifestList(materializer.fileIo().newInputFile(manifestListPath));
    ManifestFile dataManifest =
        manifests.stream()
            .filter(manifest -> manifest.content() == ManifestContent.DATA)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Data manifest not found in manifest list"));
    ManifestFile deleteManifest =
        manifests.stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));

    EntrySequences dataEntry = readSingleDataEntrySequences(materializer.fileIo(), dataManifest);
    assertEquals(1L, dataEntry.dataSequenceNumber());
    assertEquals(1L, dataEntry.fileSequenceNumber());

    EntrySequences deleteEntry =
        readSingleDeleteEntrySequences(
            materializer.fileIo(),
            deleteManifest,
            Map.of(deleteManifest.partitionSpecId(), PartitionSpec.unpartitioned()));
    assertEquals(1L, deleteEntry.dataSequenceNumber());
    assertEquals(1L, deleteEntry.fileSequenceNumber());
  }

  @Test
  void materializePropagatesTransformedPartitionToDeleteFiles() throws Exception {
    String schemaJson =
        "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false}],\"last-column-id\":1}";
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(14L)
            .setSequenceNumber(14L)
            .setSchemaJson(schemaJson)
            .setPartitionSpec(
                PartitionSpecInfo.newBuilder()
                    .setSpecId(9)
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(1)
                            .setName("id_bucket")
                            .setTransform("bucket[8]")
                            .build())
                    .build())
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .setSchemaJson(schemaJson)
            .putProperties("storage_location", "s3://floecat-delta/call_center/delete-partitioned")
            .build();
    ScanFile dataFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/data-bucket.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(111)
            .setRecordCount(7)
            .setPartitionSpecId(9)
            .setPartitionDataJson("{\"partitionValues\":[{\"id\":\"id\",\"value\":3}]}")
            .addDeleteFileIndices(0)
            .build();
    ScanFile deleteFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/deletes-bucket.avro")
            .setFileFormat("AVRO")
            .setFileContent(
                ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES)
            .setFileSizeInBytes(12)
            .setRecordCount(2)
            .setPartitionSpecId(9)
            .setPartitionDataJson("{\"partitionValues\":[{\"id\":\"id\",\"value\":3}]}")
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(dataFile)
                        .addDeleteFiles(deleteFile)
                        .build())
                .build());

    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    ManifestFile deleteManifest =
        readManifestList(materializer.fileIo().newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));
    Schema schema = org.apache.iceberg.SchemaParser.fromJson(schemaJson);
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(9).bucket("id", 8, "id_bucket").build();
    List<DeleteFile> deleteFiles =
        readDeleteFilesFromManifest(
            materializer.fileIo(), deleteManifest, Map.of(deleteManifest.partitionSpecId(), spec));
    assertEquals(1, deleteFiles.size());
    assertEquals(3, deleteFiles.get(0).partition().get(0, Integer.class));
  }

  @Test
  void materializeUsesUnpartitionedDeleteManifestSpecForFallbackDvDeletes() throws Exception {
    TestDeltaManifestMaterializerWithDeletes withDeletes =
        new TestDeltaManifestMaterializerWithDeletes();
    withDeletes.queryClient = queryClient;
    withDeletes.querySchemaClient = querySchemaClient;
    String schemaJson =
        "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"region\",\"type\":\"string\",\"required\":false}],\"last-column-id\":1}";
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .setSchemaJson(schemaJson)
            .putProperties("storage_location", "s3://floecat-delta/call_center/fallback-dv")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(15L)
            .setSequenceNumber(15L)
            .setSchemaJson(schemaJson)
            .setPartitionSpec(
                PartitionSpecInfo.newBuilder()
                    .setSpecId(7)
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(1)
                            .setName("region")
                            .setTransform("identity")
                            .build())
                    .build())
            .build();

    List<Snapshot> out = withDeletes.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());
    ManifestFile deleteManifest =
        readManifestList(withDeletes.fileIo().newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));
    assertEquals(0, deleteManifest.partitionSpecId());
  }

  @Test
  void materializeSkipsDeletePartitionAssignmentOnPartitionSpecMismatch() throws Exception {
    String schemaJson =
        "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false}],\"last-column-id\":1}";
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(16L)
            .setSequenceNumber(16L)
            .setSchemaJson(schemaJson)
            .setPartitionSpec(
                PartitionSpecInfo.newBuilder()
                    .setSpecId(9)
                    .addFields(
                        ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                            .setFieldId(1)
                            .setName("id_bucket")
                            .setTransform("bucket[8]")
                            .build())
                    .build())
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .setSchemaJson(schemaJson)
            .putProperties(
                "storage_location", "s3://floecat-delta/call_center/delete-spec-mismatch")
            .build();
    ScanFile dataFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/data-bucket-2.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(111)
            .setRecordCount(7)
            .setPartitionSpecId(9)
            .setPartitionDataJson("{\"partitionValues\":[{\"id\":\"id_bucket\",\"value\":3}]}")
            .addDeleteFileIndices(0)
            .build();
    ScanFile deleteFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/deletes-bucket-2.avro")
            .setFileFormat("AVRO")
            .setFileContent(
                ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES)
            .setFileSizeInBytes(12)
            .setRecordCount(2)
            .setPartitionSpecId(99)
            .setPartitionDataJson("{\"partitionValues\":[{\"id\":\"id_bucket\",\"value\":3}]}")
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(dataFile)
                        .addDeleteFiles(deleteFile)
                        .build())
                .build());

    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    String manifestListPath = out.get(0).getManifestList();
    assertFalse(manifestListPath.isBlank());

    ManifestFile deleteManifest =
        readManifestList(materializer.fileIo().newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DELETES)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Delete manifest not found in manifest list"));
    Schema schema = org.apache.iceberg.SchemaParser.fromJson(schemaJson);
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(9).bucket("id", 8, "id_bucket").build();
    List<DeleteFile> deleteFiles =
        readDeleteFilesFromManifest(
            materializer.fileIo(), deleteManifest, Map.of(deleteManifest.partitionSpecId(), spec));
    assertEquals(1, deleteFiles.size());
    assertEquals(3, deleteFiles.get(0).partition().get(0, Integer.class));
  }

  @Test
  void materializeSkipsDeltaEngineWhenDeltaLogMissing() {
    TestDeltaManifestMaterializerWithoutDeltaLog noDeltaLog =
        new TestDeltaManifestMaterializerWithoutDeltaLog();
    noDeltaLog.queryClient = queryClient;
    noDeltaLog.querySchemaClient = querySchemaClient;

    Table table = deltaTable("no-delta-log");
    Snapshot snapshot = snapshot(10L, 10L);

    List<Snapshot> out = noDeltaLog.materialize(table, List.of(snapshot));
    assertFalse(out.get(0).getManifestList().isBlank());
  }

  @Test
  void loadDeltaPositionDeleteFilesIgnoresUnreferencedFixtureDeletionVectorFile() throws Exception {
    TestDeltaManifestMaterializerWithFixture fixtureMaterializer =
        new TestDeltaManifestMaterializerWithFixture();
    fixtureMaterializer.queryClient = queryClient;
    fixtureMaterializer.querySchemaClient = querySchemaClient;

    String tableRoot = fixtureMaterializer.stageFixture("delta-fixtures/dv_demo_delta");

    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .putProperties("storage_location", tableRoot)
            .build();
    Snapshot snapshot = snapshot(3L, 3L);

    List<DeleteFile> deleteFiles = fixtureMaterializer.loadDeleteFiles(table, snapshot);
    assertTrue(deleteFiles.isEmpty());
  }

  private Table deltaTable(String testName) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
        .putProperties("storage_location", "s3://floecat-delta/call_center/" + testName)
        .build();
  }

  private Snapshot snapshot(long snapshotId, long sequence) {
    return Snapshot.newBuilder().setSnapshotId(snapshotId).setSequenceNumber(sequence).build();
  }

  private static DataFile readSingleDataFileFromManifestList(
      ReopenableInMemoryFileIO fileIo, String manifestListPath) throws Exception {
    ManifestFile dataManifest =
        readManifestList(fileIo.newInputFile(manifestListPath)).stream()
            .filter(manifest -> manifest.content() == ManifestContent.DATA)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Data manifest not found in manifest list"));
    return readSingleDataFileFromManifest(fileIo, dataManifest);
  }

  private static DataFile readSingleDataFileFromManifest(
      ReopenableInMemoryFileIO fileIo, ManifestFile dataManifest) throws Exception {
    try (var reader = ManifestFiles.read(dataManifest, fileIo);
        var files = reader.iterator()) {
      assertTrue(files.hasNext(), "Expected one data file entry in compat manifest");
      DataFile out = files.next().copy();
      assertFalse(files.hasNext(), "Expected exactly one data file entry in compat manifest");
      return out;
    }
  }

  @SuppressWarnings("unchecked")
  private static List<ManifestFile> readManifestList(InputFile inputFile) throws Exception {
    Class<?> manifestLists = Class.forName("org.apache.iceberg.ManifestLists");
    var read = manifestLists.getDeclaredMethod("read", InputFile.class);
    read.setAccessible(true);
    return (List<ManifestFile>) read.invoke(null, inputFile);
  }

  private static List<DeleteFile> readDeleteFilesFromManifest(
      ReopenableInMemoryFileIO fileIo, ManifestFile manifest, Map<Integer, PartitionSpec> specsById)
      throws Exception {
    try (var reader = ManifestFiles.readDeleteManifest(manifest, fileIo, specsById);
        var iter = reader.iterator()) {
      List<DeleteFile> files = new java.util.ArrayList<>();
      while (iter.hasNext()) {
        files.add(iter.next().copy());
      }
      return files;
    }
  }

  private static EntrySequences readSingleDataEntrySequences(
      ReopenableInMemoryFileIO fileIo, ManifestFile manifest) throws Exception {
    try (var reader = ManifestFiles.read(manifest, fileIo)) {
      return readSingleEntrySequences(reader);
    }
  }

  private static EntrySequences readSingleDeleteEntrySequences(
      ReopenableInMemoryFileIO fileIo, ManifestFile manifest, Map<Integer, PartitionSpec> specsById)
      throws Exception {
    try (var reader = ManifestFiles.readDeleteManifest(manifest, fileIo, specsById)) {
      return readSingleEntrySequences(reader);
    }
  }

  private static EntrySequences readSingleEntrySequences(Object manifestReader) throws Exception {
    Method entries = manifestReader.getClass().getDeclaredMethod("entries");
    entries.setAccessible(true);
    @SuppressWarnings("unchecked")
    CloseableIterable<Object> entryIterable =
        (CloseableIterable<Object>) entries.invoke(manifestReader);
    try (entryIterable) {
      try (var iterator = entryIterable.iterator()) {
        assertTrue(iterator.hasNext(), "Expected one manifest entry");
        Object entry = iterator.next();
        Method dataSequenceNumber = entry.getClass().getDeclaredMethod("dataSequenceNumber");
        Method fileSequenceNumber = entry.getClass().getDeclaredMethod("fileSequenceNumber");
        dataSequenceNumber.setAccessible(true);
        fileSequenceNumber.setAccessible(true);
        Long dataSeq = (Long) dataSequenceNumber.invoke(entry);
        Long fileSeq = (Long) fileSequenceNumber.invoke(entry);
        assertFalse(iterator.hasNext(), "Expected exactly one manifest entry");
        return new EntrySequences(dataSeq, fileSeq);
      }
    }
  }

  private record EntrySequences(Long dataSequenceNumber, Long fileSequenceNumber) {}

  private static class TestDeltaManifestMaterializer extends DeltaManifestMaterializer {
    private final ReopenableInMemoryFileIO fileIo = new ReopenableInMemoryFileIO();

    @Override
    protected FileIO newFileIo(Table table) {
      return fileIo;
    }

    protected ReopenableInMemoryFileIO fileIo() {
      return fileIo;
    }
  }

  private static final class TestDeltaManifestMaterializerWithDeletes
      extends TestDeltaManifestMaterializer {
    @Override
    protected List<DeleteFile> loadDeltaPositionDeleteFiles(
        FileIO icebergFileIo, Table table, Snapshot snapshot, String metadataRoot) {
      DeleteFile deleteFile =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath(metadataRoot + "/" + snapshot.getSnapshotId() + "-compat-pd-test.avro")
              .withFormat(FileFormat.AVRO)
              .withFileSizeInBytes(1L)
              .withRecordCount(1L)
              .withReferencedDataFile("s3://floecat-delta/call_center/data/part-00000.parquet")
              .build();
      return List.of(deleteFile);
    }
  }

  private static final class TestDeltaManifestMaterializerWithoutDeltaLog
      extends TestDeltaManifestMaterializer {
    @Override
    protected Engine newDeltaEngine(FileIO icebergFileIo) {
      throw new AssertionError("newDeltaEngine must not be called when _delta_log is absent");
    }
  }

  private static final class TestDeltaManifestMaterializerWithFixture
      extends DeltaManifestMaterializer {
    private final HadoopFileIO fileIo = new HadoopFileIO();

    private TestDeltaManifestMaterializerWithFixture() {
      fileIo.initialize(Map.of());
    }

    @Override
    protected FileIO newFileIo(Table table) {
      return fileIo;
    }

    List<DeleteFile> loadDeleteFiles(Table table, Snapshot snapshot) throws Exception {
      String metadataRoot = table.getPropertiesMap().get("storage_location") + "/metadata";
      return loadDeltaPositionDeleteFiles(fileIo, table, snapshot, metadataRoot);
    }

    String stageFixture(String resourceDir) throws Exception {
      URI resourceUri =
          Objects.requireNonNull(
                  Thread.currentThread().getContextClassLoader().getResource(resourceDir),
                  "Fixture directory not found: " + resourceDir)
              .toURI();
      FileSystem resourceFs = null;
      boolean closeResourceFs = false;
      Path fixtureRoot;
      if ("jar".equalsIgnoreCase(resourceUri.getScheme())) {
        try {
          resourceFs = FileSystems.getFileSystem(resourceUri);
        } catch (FileSystemNotFoundException e) {
          resourceFs = FileSystems.newFileSystem(resourceUri, Map.of());
          closeResourceFs = true;
        }
        fixtureRoot =
            resourceFs.getPath(resourceDir.startsWith("/") ? resourceDir : "/" + resourceDir);
      } else {
        fixtureRoot = Path.of(resourceUri);
      }
      Path stagedRoot = Files.createTempDirectory("delta-dv-fixture-");
      try {
        try (var paths = Files.walk(fixtureRoot)) {
          for (Path source : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
            String relative = fixtureRoot.relativize(source).toString().replace('\\', '/');
            if (relative.endsWith(".crc")) {
              continue;
            }
            Path target = stagedRoot.resolve(relative);
            Files.createDirectories(target.getParent());
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      } finally {
        if (resourceFs != null && closeResourceFs) {
          resourceFs.close();
        }
      }
      return stagedRoot.toUri().toString().replaceAll("/$", "");
    }
  }

  private static final class ReopenableInMemoryFileIO extends InMemoryFileIO {
    @Override
    public void close() {
      // Keep files available across multiple materialize() calls in the same test.
    }
  }
}
