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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeltaManifestMaterializerTest {
  private static final String UNPARTITIONED_SCHEMA_JSON =
      "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false},{\"id\":2,\"name\":\"v\",\"type\":\"string\",\"required\":false}],\"last-column-id\":2}";
  private static final String PARTITIONED_SCHEMA_JSON =
      "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false},{\"id\":2,\"name\":\"region\",\"type\":\"string\",\"required\":false},{\"id\":3,\"name\":\"dt\",\"type\":\"date\",\"required\":false},{\"id\":4,\"name\":\"payload\",\"type\":\"string\",\"required\":false}],\"last-column-id\":4}";
  private static final String TRANSFORMED_SCHEMA_JSON =
      "{\"schema-id\":1,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false},{\"id\":2,\"name\":\"name\",\"type\":\"string\",\"required\":false},{\"id\":3,\"name\":\"ts\",\"type\":\"timestamp\",\"required\":false},{\"id\":4,\"name\":\"bucket16\",\"type\":\"int\",\"required\":false},{\"id\":5,\"name\":\"name_trunc4\",\"type\":\"string\",\"required\":false},{\"id\":6,\"name\":\"ts_year\",\"type\":\"int\",\"required\":false}],\"last-column-id\":6}";
  private static final String VARIANT_SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"payload\",\"type\":\"variant\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4}}]}";

  private final GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);

  @BeforeEach
  void setUp() {}

  @Test
  void materializeReadsDataFilesDirectlyFromDeltaLog() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    materializer.grpcClient = grpcClient;
    String tableRoot = materializer.stageFixture("delta-fixtures/01_unpartitioned_append_only");
    Table table = deltaTable(tableRoot, UNPARTITIONED_SCHEMA_JSON);
    Snapshot snapshot = snapshot(1L, UNPARTITIONED_SCHEMA_JSON, null);
    DeltaManifestMaterializer.CompatSnapshotFiles snapshotFiles =
        materializer.loadSnapshotFiles(
            table,
            snapshot,
            PartitionSpec.unpartitioned(),
            SchemaParser.fromJson(UNPARTITIONED_SCHEMA_JSON));

    List<DataFile> dataFiles = snapshotFiles.dataFiles();
    assertEquals(2, dataFiles.size());
    long totalRecordCount = dataFiles.stream().mapToLong(DataFile::recordCount).sum();
    assertEquals(40L, totalRecordCount);

    DataFile firstFile =
        dataFiles.stream()
            .filter(file -> file.location().contains("part-00000"))
            .findFirst()
            .orElseThrow();
    assertEquals(20L, firstFile.recordCount());
    assertEquals(
        0,
        (int) Conversions.fromByteBuffer(Types.IntegerType.get(), firstFile.lowerBounds().get(1)));
    assertEquals(
        19,
        (int) Conversions.fromByteBuffer(Types.IntegerType.get(), firstFile.upperBounds().get(1)));
    assertEquals(
        "val-0",
        Conversions.fromByteBuffer(Types.StringType.get(), firstFile.lowerBounds().get(2))
            .toString());
    verifyNoInteractions(grpcClient);
  }

  @Test
  void materializePropagatesIdentityPartitionValuesFromDeltaLog() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    materializer.grpcClient = grpcClient;
    String tableRoot =
        materializer.stageFixture("delta-fixtures/02_partitioned_identity_region_date");
    Table table = deltaTable(tableRoot, PARTITIONED_SCHEMA_JSON);
    Snapshot snapshot =
        snapshot(
            1L,
            PARTITIONED_SCHEMA_JSON,
            PartitionSpecInfo.newBuilder()
                .setSpecId(7)
                .addFields(
                    ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                        .setFieldId(2)
                        .setName("region")
                        .setTransform("identity")
                        .build())
                .addFields(
                    ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                        .setFieldId(3)
                        .setName("dt")
                        .setTransform("identity")
                        .build())
                .build());

    DeltaManifestMaterializer.CompatSnapshotFiles snapshotFiles =
        materializer.loadSnapshotFiles(
            table,
            snapshot,
            specFromSnapshot(PARTITIONED_SCHEMA_JSON, snapshot.getPartitionSpec()),
            SchemaParser.fromJson(PARTITIONED_SCHEMA_JSON));
    List<DataFile> dataFiles = snapshotFiles.dataFiles();

    assertEquals(12, dataFiles.size());
    assertTrue(
        dataFiles.stream()
            .anyMatch(
                file ->
                    "us-east".equals(file.partition().get(0, String.class))
                        && (int) LocalDate.of(2026, 2, 1).toEpochDay()
                            == file.partition().get(1, Integer.class)));
    verifyNoInteractions(grpcClient);
  }

  @Test
  void materializePropagatesPartitionColumnsFromTransformedFixture() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    materializer.grpcClient = grpcClient;
    String tableRoot =
        materializer.stageFixture("delta-fixtures/03_partitioned_transformed_emulated");
    Table table = deltaTable(tableRoot, TRANSFORMED_SCHEMA_JSON);
    Snapshot snapshot =
        snapshot(
            1L,
            TRANSFORMED_SCHEMA_JSON,
            PartitionSpecInfo.newBuilder()
                .setSpecId(8)
                .addFields(
                    ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                        .setFieldId(4)
                        .setName("bucket16")
                        .setTransform("identity")
                        .build())
                .addFields(
                    ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                        .setFieldId(5)
                        .setName("name_trunc4")
                        .setTransform("identity")
                        .build())
                .addFields(
                    ai.floedb.floecat.catalog.rpc.PartitionField.newBuilder()
                        .setFieldId(6)
                        .setName("ts_year")
                        .setTransform("identity")
                        .build())
                .build());

    DeltaManifestMaterializer.CompatSnapshotFiles snapshotFiles =
        materializer.loadSnapshotFiles(
            table,
            snapshot,
            specFromSnapshot(TRANSFORMED_SCHEMA_JSON, snapshot.getPartitionSpec()),
            SchemaParser.fromJson(TRANSFORMED_SCHEMA_JSON));
    List<DataFile> dataFiles = snapshotFiles.dataFiles();

    assertEquals(31, dataFiles.size());
    assertTrue(
        dataFiles.stream()
            .anyMatch(
                file ->
                    file.partition().get(0, Integer.class) == 15
                        && "name".equals(file.partition().get(1, String.class))
                        && file.partition().get(2, Integer.class) == 2026));
    verifyNoInteractions(grpcClient);
  }

  @Test
  void materializeWritesDeleteManifestFromDeltaDeletionVectors() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    materializer.grpcClient = grpcClient;
    String tableRoot = materializer.stageFixture("delta-fixtures/10_deletion_vectors");
    Table table = deltaTable(tableRoot, UNPARTITIONED_SCHEMA_JSON);
    Snapshot snapshot = snapshot(3L, UNPARTITIONED_SCHEMA_JSON, null);

    DeltaManifestMaterializer.CompatSnapshotFiles snapshotFiles =
        materializer.loadSnapshotFiles(
            table,
            snapshot,
            PartitionSpec.unpartitioned(),
            SchemaParser.fromJson(UNPARTITIONED_SCHEMA_JSON));
    List<DeleteFile> deleteFiles = snapshotFiles.deleteFiles();
    assertEquals(1, deleteFiles.size());
    assertEquals(FileContent.POSITION_DELETES, deleteFiles.get(0).content());
    assertTrue(deleteFiles.get(0).location().contains("compat-pd-"));
    verifyNoInteractions(grpcClient);
  }

  @Test
  void materializeLeavesSnapshotUnchangedWhenDeltaLogMissing() {
    NoDeltaLogMaterializer materializer = new NoDeltaLogMaterializer();
    materializer.grpcClient = grpcClient;
    Table table = deltaTable("file:///tmp/no-delta-log", UNPARTITIONED_SCHEMA_JSON);
    Snapshot snapshot = snapshot(10L, UNPARTITIONED_SCHEMA_JSON, null);

    List<Snapshot> out = materializer.materialize(table, List.of(snapshot));
    assertTrue(out.get(0).getManifestList().isBlank());
    verifyNoInteractions(grpcClient);
  }

  @Test
  void fileFormatForPathFallsBackToParquetForExtensionlessDeltaDataPath() {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();

    FileFormat format =
        materializer.fileFormatForPath(
            "s3://floecat-delta/call_center/20250825_183517_00001_s25in_55937b16-9009-4a18-81ea-5a83e97eca53");

    assertEquals(FileFormat.PARQUET, format);
  }

  @Test
  void compatCommitTreatsExistingCommittedMetadataAsSuccess() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    materializer.grpcClient = grpcClient;
    String tableRoot = materializer.stageFixture("delta-fixtures/01_unpartitioned_append_only");
    Table table = deltaTable(tableRoot, UNPARTITIONED_SCHEMA_JSON);
    Snapshot snapshot = snapshot(1L, UNPARTITIONED_SCHEMA_JSON, null);

    List<Snapshot> materialized = materializer.materialize(table, List.of(snapshot));
    assertTrue(!materialized.get(0).getManifestList().isBlank());

    String metadataRoot = "s3://delta-compat-tests/1/metadata";
    String metadataPath = metadataRoot + "/compat-1.metadata.json";
    TableMetadata existing = TableMetadataParser.read(materializer.fileIo(), metadataPath);
    assertNotNull(existing);
    assertNotNull(existing.currentSnapshot());

    Class<?> opsClass = null;
    for (Class<?> candidate : DeltaManifestMaterializer.class.getDeclaredClasses()) {
      if ("CompatTableOperations".equals(candidate.getSimpleName())) {
        opsClass = candidate;
        break;
      }
    }
    assertNotNull(opsClass);
    var ctor =
        opsClass.getDeclaredConstructor(FileIO.class, String.class, String.class, String.class);
    ctor.setAccessible(true);
    TableOperations ops =
        (TableOperations)
            ctor.newInstance(
                materializer.fileIo(), "s3://delta-compat-tests/1", metadataRoot, metadataPath);

    TableMetadata replacement =
        TableMetadata.newTableMetadata(
            existing.schema(),
            existing.spec(),
            SortOrder.unsorted(),
            "s3://delta-compat-tests/1",
            Map.of());
    ops.commit(existing, replacement);

    TableMetadata current = ops.current();
    assertNotNull(current);
    assertNotNull(current.currentSnapshot());
    assertEquals(
        existing.currentSnapshot().manifestListLocation(),
        current.currentSnapshot().manifestListLocation());
  }

  @Test
  void parseSnapshotSchemaPreservesVariantWhenFallbackDisabled() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    IcebergGatewayConfig gatewayConfig = mock(IcebergGatewayConfig.class);
    IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
        mock(IcebergGatewayConfig.DeltaCompatConfig.class);
    when(gatewayConfig.deltaCompat()).thenReturn(java.util.Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.rewriteVariantAsStruct()).thenReturn(false);
    materializer.gatewayConfig = gatewayConfig;

    Table table = deltaTable("file:///tmp/variant-table", VARIANT_SCHEMA_JSON);
    Snapshot snapshot = snapshot(1L, VARIANT_SCHEMA_JSON, null);

    Method method =
        DeltaManifestMaterializer.class.getDeclaredMethod(
            "parseSnapshotSchema", Snapshot.class, Table.class);
    method.setAccessible(true);
    Schema schema = (Schema) method.invoke(materializer, snapshot, table);

    assertEquals(
        org.apache.iceberg.types.Type.TypeID.VARIANT, schema.findField("payload").type().typeId());
  }

  @Test
  void parseSnapshotSchemaPreservesVariantWhenDeltaCompatRewriteConfigMissing() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    IcebergGatewayConfig gatewayConfig = mock(IcebergGatewayConfig.class);
    when(gatewayConfig.deltaCompat()).thenReturn(java.util.Optional.empty());
    materializer.gatewayConfig = gatewayConfig;

    Table table = deltaTable("file:///tmp/variant-table", VARIANT_SCHEMA_JSON);
    Snapshot snapshot = snapshot(1L, VARIANT_SCHEMA_JSON, null);

    Method method =
        DeltaManifestMaterializer.class.getDeclaredMethod(
            "parseSnapshotSchema", Snapshot.class, Table.class);
    method.setAccessible(true);
    Schema schema = (Schema) method.invoke(materializer, snapshot, table);

    assertEquals(
        org.apache.iceberg.types.Type.TypeID.VARIANT, schema.findField("payload").type().typeId());
  }

  @Test
  void metadataRootPrefersStorageLocationOverWorkspaceLocation() throws Exception {
    FixtureDeltaManifestMaterializer materializer = new FixtureDeltaManifestMaterializer();
    String tableRoot = materializer.stageFixture("delta-fixtures/01_unpartitioned_append_only");
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
            .putProperties("location", "https://dbc-d6b5397e-c401.cloud.databricks.com")
            .putProperties("storage_location", tableRoot)
            .build();

    String metadataRoot = materializer.metadataRootForTest(table);
    assertEquals(tableRoot + "/metadata", metadataRoot);
  }

  private static Table deltaTable(String tableRoot, String schemaJson) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
        .setSchemaJson(schemaJson)
        .putProperties("storage_location", tableRoot)
        .build();
  }

  private static Snapshot snapshot(
      long snapshotId, String schemaJson, PartitionSpecInfo partitionSpec) {
    Snapshot.Builder builder =
        Snapshot.newBuilder()
            .setSnapshotId(snapshotId)
            .setSequenceNumber(snapshotId)
            .setSchemaJson(schemaJson);
    if (partitionSpec != null) {
      builder.setPartitionSpec(partitionSpec);
    }
    return builder.build();
  }

  private static PartitionSpec specFromSnapshot(String schemaJson, PartitionSpecInfo specInfo) {
    if (specInfo == null || specInfo.getFieldsCount() == 0) {
      return PartitionSpec.unpartitioned();
    }
    Schema schema = SchemaParser.fromJson(schemaJson);
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).withSpecId(specInfo.getSpecId());
    for (ai.floedb.floecat.catalog.rpc.PartitionField field : specInfo.getFieldsList()) {
      String sourceName = schema.findColumnName(field.getFieldId());
      assertNotNull(sourceName);
      String transform = field.getTransform();
      if ("identity".equals(transform)) {
        builder.identity(sourceName, field.getName());
        continue;
      }
      throw new IllegalArgumentException("Unsupported test transform: " + transform);
    }
    return builder.build();
  }

  private static class FixtureDeltaManifestMaterializer extends DeltaManifestMaterializer {
    private final LocalFixtureFileIO sourceFileIo = new LocalFixtureFileIO();
    private final NonClosingInMemoryFileIO compatFileIo = new NonClosingInMemoryFileIO();

    private FixtureDeltaManifestMaterializer() {
      sourceFileIo.initialize(Map.of());
      compatFileIo.initialize(Map.of());
    }

    @Override
    protected FileIO newFileIo(Table table) {
      return sourceFileIo;
    }

    @Override
    protected FileIO newSourceFileIo(Table table, String metadataRoot) {
      return sourceFileIo;
    }

    @Override
    protected CompatStorageContext resolveCompatStorage(Table table, Snapshot snapshot) {
      return new CompatStorageContext(
          "s3://delta-compat-tests/" + snapshot.getSnapshotId() + "/metadata", Map.of());
    }

    @Override
    protected FileIO newCompatFileIo(Table table, Snapshot snapshot, CompatStorageContext compat) {
      return compatFileIo;
    }

    FileIO fileIo() {
      return compatFileIo;
    }

    String metadataRootForTest(Table table) throws Exception {
      Method method =
          DeltaManifestMaterializer.class.getDeclaredMethod("metadataRoot", Table.class);
      method.setAccessible(true);
      return (String) method.invoke(this, table);
    }

    CompatSnapshotFiles loadSnapshotFiles(
        Table table, Snapshot snapshot, PartitionSpec spec, Schema schema) throws Exception {
      return loadCompatSnapshotFiles(
          compatFileIo,
          new SourceTableAccess(Map.of(), sourceFileIo),
          table,
          snapshot,
          "s3://delta-compat-tests/" + snapshot.getSnapshotId() + "/metadata",
          table.getPropertiesMap().get("storage_location") + "/metadata",
          spec,
          schema);
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
      Path stagedRoot = Files.createTempDirectory("delta-fixture-");
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
      return toUri(stagedRoot);
    }
  }

  private static final class NoDeltaLogMaterializer extends FixtureDeltaManifestMaterializer {}

  private static final class NonClosingInMemoryFileIO extends InMemoryFileIO {
    @Override
    public void close() {}
  }

  private static final class LocalFixtureFileIO implements SupportsPrefixOperations {
    @Override
    public InputFile newInputFile(String location) {
      Path path = toLocalPath(location);
      return new InputFile() {
        @Override
        public long getLength() {
          try {
            return Files.size(path);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public SeekableInputStream newStream() {
          try {
            SeekableByteChannel channel = Files.newByteChannel(path);
            java.io.InputStream in = Channels.newInputStream(channel);
            return new SeekableInputStream() {
              @Override
              public int read() throws java.io.IOException {
                return in.read();
              }

              @Override
              public int read(byte[] b, int off, int len) throws java.io.IOException {
                return in.read(b, off, len);
              }

              @Override
              public long getPos() throws java.io.IOException {
                return channel.position();
              }

              @Override
              public void seek(long newPos) throws java.io.IOException {
                channel.position(newPos);
              }

              @Override
              public void close() throws java.io.IOException {
                in.close();
              }
            };
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public String location() {
          return normalizeFileUri(path.toUri().toString());
        }

        @Override
        public boolean exists() {
          return Files.exists(path);
        }
      };
    }

    @Override
    public OutputFile newOutputFile(String location) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String location) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      try {
        Path root = toLocalPath(prefix);
        if (!Files.exists(root)) {
          return List.of();
        }
        try (var paths = Files.list(root)) {
          return paths
              .filter(Files::isRegularFile)
              .map(
                  path -> {
                    try {
                      return new FileInfo(
                          normalizeFileUri(path.toUri().toString()),
                          Files.size(path),
                          Files.getLastModifiedTime(path).toMillis());
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .toList();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void deletePrefix(String prefix) {
      throw new UnsupportedOperationException();
    }

    private Path toLocalPath(String location) {
      if (location.startsWith("file:")) {
        return Path.of(URI.create(location));
      }
      return Path.of(location);
    }
  }

  private static String toUri(Path path) {
    return normalizeFileUri(path.toUri().toString()).replaceAll("/$", "");
  }

  private static String normalizeFileUri(String value) {
    return value.replace("file:///", "file:/");
  }
}
