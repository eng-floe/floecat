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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
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
    assertTrue(firstManifestList.endsWith("/metadata/snap-7-compat.avro"));

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
    assertTrue(newManifestList.endsWith("/metadata/snap-8-compat.avro"));

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
    assertTrue(firstManifestList.endsWith("/metadata/snap-7-compat.avro"));
    assertTrue(secondManifestList.endsWith("/metadata/snap-8-compat.avro"));

    List<Snapshot> second = materializer.materialize(table, List.of(firstSnapshot, secondSnapshot));
    assertEquals(firstManifestList, second.get(0).getManifestList());
    assertEquals(secondManifestList, second.get(1).getManifestList());

    verify(queryClient, times(2)).fetchScanBundle(any());
  }

  @Test
  void materializeWritesDeleteManifestWhenDeleteFilesPresent() {
    TestDeltaManifestMaterializerWithDeletes withDeletes =
        new TestDeltaManifestMaterializerWithDeletes();
    withDeletes.queryClient = queryClient;
    withDeletes.querySchemaClient = querySchemaClient;

    Table table = deltaTable("with-deletes");
    Snapshot snapshot = snapshot(9L, 9L);

    List<Snapshot> out = withDeletes.materialize(table, List.of(snapshot));
    String manifestList = out.get(0).getManifestList();
    assertFalse(manifestList.isBlank());

    String deleteManifestPath =
        "s3://floecat-delta/call_center/with-deletes/metadata/9-compat-d0.avro";
    assertTrue(withDeletes.fileIo().newInputFile(deleteManifestPath).exists());
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
