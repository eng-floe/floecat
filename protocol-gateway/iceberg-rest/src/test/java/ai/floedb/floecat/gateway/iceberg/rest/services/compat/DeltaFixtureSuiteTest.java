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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestDeltaFixtures;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import io.delta.kernel.engine.Engine;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DeltaFixtureSuiteTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final List<String> SUITE_TABLES =
      List.of(
          "01_unpartitioned_append_only",
          "02_partitioned_identity_region_date",
          "03_partitioned_transformed_emulated",
          "04_partition_evolution_replace_table",
          "05_schema_evolution",
          "06_deletes_only",
          "07_equality_deletes",
          "08_mixed_delete",
          "09_cdc_updates_merges_heavy",
          "10_deletion_vectors",
          "11_null_heavy_skewed_extremes");

  private final DeltaIcebergMetadataTranslator translator = new DeltaIcebergMetadataTranslator();

  @BeforeAll
  static void seedDeltaFixtures() {
    TestDeltaFixtures.seedFixturesOnce();
  }

  @Test
  void translatorBuildsIcebergMetadataAcrossFixtureSuite() throws Exception {
    try (StagedFixtureRoot staged = stageFixtureRoot("delta-fixtures")) {
      Path fixtureRoot = staged.root();
      Set<String> actual = listTableDirs(fixtureRoot);
      assertTrue(actual.containsAll(SUITE_TABLES), "Missing generated fixture tables");

      for (String tableName : SUITE_TABLES) {
        Path tableDir = fixtureRoot.resolve(tableName);
        List<Snapshot> snapshots = loadSnapshotsFromDeltaLog(tableDir.resolve("_delta_log"));
        assertFalse(snapshots.isEmpty(), "Expected snapshot history for " + tableName);
        Snapshot latest = snapshots.get(snapshots.size() - 1);
        String latestSchema = latest.getSchemaJson();
        assertFalse(latestSchema.isBlank(), "Expected schema JSON for " + tableName);

        Table table = tableForFixture(tableName).toBuilder().setSchemaJson(latestSchema).build();
        var metadata = translator.translate(table, snapshots);

        assertEquals(latest.getSnapshotId(), metadata.getCurrentSnapshotId(), tableName);
        assertEquals(latest.getSequenceNumber(), metadata.getLastSequenceNumber(), tableName);
        assertEquals(snapshots.size(), metadata.getSnapshotLogCount(), tableName);
        assertEquals(
            latest.getSnapshotId(), metadata.getRefsOrThrow("main").getSnapshotId(), tableName);
        assertTrue(metadata.getSchemasCount() >= 1, "Expected translated schema for " + tableName);

        var schema =
            org.apache.iceberg.SchemaParser.fromJson(metadata.getSchemas(0).getSchemaJson());
        assertFalse(
            schema.columns().isEmpty(), "Expected non-empty translated columns for " + tableName);

        if ("05_schema_evolution".equals(tableName)) {
          assertTrue(schema.findField("full_name") != null, "Expected renamed column full_name");
          assertTrue(schema.findField("name") == null, "Expected old column name to be absent");
        }
        if ("10_deletion_vectors".equals(tableName)) {
          assertTrue(
              schema.findType("id") == Types.IntegerType.get(),
              "Expected stable integer id column in DV fixture");
        }
      }
    }
  }

  @Test
  void dvLoaderReturnsDeletesOnlyForDeletionVectorFixture() throws Exception {
    try (StagedFixtureRoot staged = stageFixtureRoot("delta-fixtures");
        FixtureMaterializer materializer = new FixtureMaterializer()) {
      Path fixtureRoot = staged.root();
      for (String tableName : SUITE_TABLES) {
        Path tableDir = fixtureRoot.resolve(tableName);
        long latestVersion = latestLogVersion(tableDir.resolve("_delta_log"));
        Table table = tableForFixture(tableName);
        Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(latestVersion).build();
        List<DeleteFile> deletes = materializer.loadDeleteFiles(table, snapshot);
        if (hasOnDiskDvEntries(tableDir.resolve("_delta_log"))) {
          assertFalse(deletes.isEmpty(), "Expected DV-derived position deletes for " + tableName);
        } else {
          assertTrue(deletes.isEmpty(), "Expected no DV-derived position deletes for " + tableName);
        }
      }
    }
  }

  private StagedFixtureRoot stageFixtureRoot(String resourceDir) throws Exception {
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

    Path stagedRoot = Files.createTempDirectory("delta-fixtures-suite-");
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
    return new StagedFixtureRoot(stagedRoot, resourceFs, closeResourceFs);
  }

  private Set<String> listTableDirs(Path fixtureRoot) throws IOException {
    try (Stream<Path> stream = Files.list(fixtureRoot)) {
      return stream
          .filter(Files::isDirectory)
          .map(path -> path.getFileName().toString())
          .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    }
  }

  private long latestLogVersion(Path deltaLog) throws IOException {
    try (Stream<Path> stream = Files.list(deltaLog)) {
      return stream
          .map(path -> path.getFileName().toString())
          .filter(name -> name.matches("\\d{20}\\.json"))
          .map(name -> name.substring(0, name.length() - 5))
          .mapToLong(Long::parseLong)
          .max()
          .orElseThrow(() -> new IllegalStateException("No Delta log JSON files in " + deltaLog));
    }
  }

  private List<Snapshot> loadSnapshotsFromDeltaLog(Path deltaLog) throws IOException {
    List<Path> commits;
    try (Stream<Path> stream = Files.list(deltaLog)) {
      commits =
          stream
              .filter(path -> path.getFileName().toString().matches("\\d{20}\\.json"))
              .sorted()
              .toList();
    }
    String schemaJson = null;
    List<Snapshot> snapshots = new ArrayList<>();
    for (Path commit : commits) {
      long version = Long.parseLong(commit.getFileName().toString().replace(".json", ""));
      long timestampMs = 0L;
      for (String line : Files.readAllLines(commit)) {
        JsonNode root = JSON.readTree(line);
        JsonNode metadata = root.get("metaData");
        if (metadata != null && metadata.has("schemaString")) {
          schemaJson = metadata.get("schemaString").asText("");
        }
        JsonNode commitInfo = root.get("commitInfo");
        if (commitInfo != null && commitInfo.has("timestamp")) {
          timestampMs = commitInfo.get("timestamp").asLong(0L);
        }
      }
      if (schemaJson == null || schemaJson.isBlank()) {
        continue;
      }
      snapshots.add(
          Snapshot.newBuilder()
              .setSnapshotId(version)
              .setSequenceNumber(version)
              .setSchemaId(1)
              .setSchemaJson(schemaJson)
              .setUpstreamCreatedAt(
                  Timestamp.newBuilder()
                      .setSeconds(timestampMs / 1000L)
                      .setNanos((int) ((timestampMs % 1000L) * 1_000_000L))
                      .build())
              .build());
    }
    return snapshots;
  }

  private boolean hasOnDiskDvEntries(Path deltaLog) throws IOException {
    try (Stream<Path> stream = Files.list(deltaLog)) {
      for (Path logFile :
          stream.filter(path -> path.getFileName().toString().matches("\\d{20}\\.json")).toList()) {
        for (String line : Files.readAllLines(logFile)) {
          JsonNode root = JSON.readTree(line);
          JsonNode addDv = root.path("add").path("deletionVector");
          JsonNode removeDv = root.path("remove").path("deletionVector");
          if (isOnDiskDv(addDv) || isOnDiskDv(removeDv)) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private boolean isOnDiskDv(JsonNode deletionVector) {
    if (deletionVector == null || deletionVector.isMissingNode() || deletionVector.isNull()) {
      return false;
    }
    String storageType = deletionVector.path("storageType").asText("");
    // Delta uses inline storage type "i"; other storage types are backed by persistent files.
    return !storageType.isBlank() && !"i".equalsIgnoreCase(storageType);
  }

  private Table tableForFixture(String tableName) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:" + tableName).build())
        .putProperties("storage_location", TestDeltaFixtures.tableUri(tableName))
        .build();
  }

  private static final class FixtureMaterializer extends DeltaManifestMaterializer
      implements AutoCloseable {
    private final FileIO fileIo;

    private FixtureMaterializer() {
      if (TestDeltaFixtures.useAwsFixtures()) {
        Map<String, String> props = new java.util.LinkedHashMap<>(TestDeltaFixtures.s3Options());
        fileIo = FileIoFactory.createFileIo(props, null, false);
      } else {
        InMemoryS3FileIO inMemory = new InMemoryS3FileIO();
        String root = System.getProperty("fs.floecat.test-root");
        inMemory.initialize(
            root == null || root.isBlank() ? Map.of() : Map.of("fs.floecat.test-root", root));
        fileIo = inMemory;
      }
    }

    @Override
    protected FileIO newFileIo(Table table) {
      return fileIo;
    }

    @Override
    protected Engine newDeltaEngine(FileIO icebergFileIo) {
      return super.newDeltaEngine(icebergFileIo);
    }

    private List<DeleteFile> loadDeleteFiles(Table table, Snapshot snapshot) throws Exception {
      String metadataRoot = table.getPropertiesMap().get("storage_location") + "/metadata";
      return loadDeltaPositionDeleteFiles(fileIo, table, snapshot, metadataRoot);
    }

    @Override
    public void close() {
      try {
        fileIo.close();
      } catch (Exception ignored) {
      }
    }
  }

  private record StagedFixtureRoot(Path root, FileSystem resourceFs, boolean closeResourceFs)
      implements AutoCloseable {
    @Override
    public void close() throws Exception {
      if (resourceFs != null && closeResourceFs) {
        resourceFs.close();
      }
    }
  }
}
