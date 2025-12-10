package ai.floedb.floecat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.support.MirrorLocationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.Test;

class MaterializeMetadataServiceTest {

  @Test
  void mirrorsMetadataAndPointerFiles() throws IOException {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://bucket/db/table",
              "metadata-location": "s3://bucket/db/table/metadata/metadata-1.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "s3.region": "us-east-1"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "last-sequence-number": 0,
              "schemas": [
                {
                  "type": "struct",
                  "schema-id": 1,
                  "last-column-id": 1,
                  "identifier-field-ids": [],
                  "fields": [
                    {
                      "id": 1,
                      "name": "id",
                      "required": true,
                      "type": "long"
                    }
                  ]
                }
              ],
              "partition-specs": [
                {
                  "spec-id": 0,
                  "fields": []
                }
              ],
              "sort-orders": [
                {
                  "order-id": 0,
                  "fields": []
                }
              ],
              "refs": {},
              "snapshot-log": [],
              "metadata-log": [],
              "statistics": [],
              "partition-statistics": [],
              "snapshots": []
            }
            """,
            TableMetadataView.class);

    MaterializeMetadataService.MaterializeResult mirrorResult =
        service.materialize("ns", "table", metadata, metadata.metadataLocation());

    String metadataJson = readFile(service.fileIO(), mirrorResult.metadataLocation());
    assertTrue(metadataJson.contains("\"table-uuid\":\"tbl-uuid\""));
    assertFalse(metadataJson.contains("/.floecat-metadata"));
    assertNotNull(service.fileIO().newInputFile(mirrorResult.metadataLocation()));
  }

  @Test
  void preservesOverrideMetadataLocation() throws IOException {
    MaterializeMetadataService service = new MaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://bucket/db/table",
              "metadata-location": "s3://bucket/db/table/metadata/metadata-1.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "s3.region": "us-east-1"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "last-sequence-number": 0,
              "metadata-log": [
                {
                  "timestamp-ms": 1,
                  "metadata-file": "s3://bucket/db/table/metadata/metadata-0.json"
                }
              ],
              "schemas": [
                {
                  "type": "struct",
                  "schema-id": 1,
                  "last-column-id": 1,
                  "identifier-field-ids": [],
                  "fields": [
                    {
                      "id": 1,
                      "name": "id",
                      "required": true,
                      "type": "long"
                    }
                  ]
                }
              ],
              "partition-specs": [
                {
                  "spec-id": 0,
                  "fields": []
                }
              ],
              "sort-orders": [
                {
                  "order-id": 0,
                  "fields": []
                }
              ],
              "refs": {},
              "snapshot-log": [],
              "statistics": [],
              "partition-statistics": [],
              "snapshots": []
            }
            """,
            TableMetadataView.class);

    String overrideLocation = "s3://bucket/db/table/metadata/00000-stage.metadata.json";
    MaterializeMetadataService.MaterializeResult result =
        service.materialize("ns", "table", metadata, overrideLocation);

    assertEquals(overrideLocation, result.metadataLocation());
  }

  @Test
  void copiesMirrorArtifactsToCanonicalLocation() throws IOException {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://floecat/sales/us/trino_order61",
              "metadata-location": "s3://floecat/sales/us/trino_order61/metadata/00001-new.metadata.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "metadata-location": "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/00001-new.metadata.json",
                "write.metadata.path": "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata",
                "s3.region": "us-east-1"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "last-sequence-number": 1,
              "schemas": [
                {
                  "type": "struct",
                  "schema-id": 1,
                  "last-column-id": 1,
                  "identifier-field-ids": [],
                  "fields": [
                    {
                      "id": 1,
                      "name": "id",
                      "required": true,
                      "type": "long"
                    }
                  ]
                }
              ],
              "partition-specs": [
                {
                  "spec-id": 0,
                  "fields": []
                }
              ],
              "sort-orders": [
                {
                  "order-id": 0,
                  "fields": []
                }
              ],
              "refs": {},
              "snapshot-log": [],
              "metadata-log": [
                {
                  "timestamp-ms": 1,
                  "metadata-file": "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/00000-old.metadata.json"
                }
              ],
              "statistics": [
                {
                  "snapshot-id": 1,
                  "statistics-path": "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/20251210_stats.stats",
                  "file-size-in-bytes": 128,
                  "file-footer-size-in-bytes": 64,
                  "blob-metadata": []
                }
              ],
              "partition-statistics": [],
              "snapshots": [
                {
                  "snapshot-id": 1,
                  "sequence-number": 1,
                  "manifest-list": "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/snap-1.avro"
                }
              ]
            }
            """,
            TableMetadataView.class);

    addStageFile(service, metadata.metadataLocation(), "metadata");
    addStageFile(service, "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/00000-old.metadata.json", "old");
    addStageFile(service, "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/snap-1.avro", "snapshot");
    addStageFile(service, "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/20251210_stats.stats", "stats");
    addStageFile(service, "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/ab2a7d3b-b083-4676-bbe5-32d3ecb0d6aa-m0.avro", "manifest");

    MaterializeMetadataService.MaterializeResult result =
        service.materialize("sales.us", "trino_order61", metadata, metadata.metadataLocation());

    String canonicalMetadata = result.metadataLocation();
    assertTrue(service.fileIO().fileExists(canonicalMetadata));
    assertTrue(
        service
            .fileIO()
            .fileExists(
                MirrorLocationUtil.stripMetadataMirrorPrefix(
                    "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/snap-1.avro")));
    assertTrue(
        service
            .fileIO()
            .fileExists(
                MirrorLocationUtil.stripMetadataMirrorPrefix(
                    "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/20251210_stats.stats")));
    assertTrue(
        service
            .fileIO()
            .fileExists(
                MirrorLocationUtil.stripMetadataMirrorPrefix(
                    "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/ab2a7d3b-b083-4676-bbe5-32d3ecb0d6aa-m0.avro")));
    String canonicalMetadata =
        readFile(
            service.fileIO(),
            MirrorLocationUtil.stripMetadataMirrorPrefix(
                "s3://floecat/.floecat-metadata/sales/us/trino_order61/metadata/00001-new.metadata.json"));
    assertFalse(canonicalMetadata.contains("/.floecat-metadata"));
  }

  @Test
  void canonicalMetadataPromotesLatestSnapshot() throws IOException {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://floecat/sales/us/trino_order70",
              "metadata-location": "s3://floecat/sales/us/trino_order70/metadata/00002.metadata.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "current-snapshot-id": "10"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "current-snapshot-id": 10,
              "last-sequence-number": 2,
              "refs": {
                "main": {
                  "snapshot-id": 10,
                  "type": "branch"
                }
              },
              "schemas": [],
              "partition-specs": [],
              "sort-orders": [],
              "snapshot-log": [],
              "metadata-log": [],
              "statistics": [],
              "partition-statistics": [],
              "snapshots": [
                {
                  "snapshot-id": 10,
                  "sequence-number": 1,
                  "manifest-list": "s3://floecat/sales/us/trino_order70/metadata/snap-10.avro"
                },
                {
                  "snapshot-id": 11,
                  "sequence-number": 2,
                  "manifest-list": "s3://floecat/sales/us/trino_order70/metadata/snap-11.avro"
                }
              ]
            }
            """,
            TableMetadataView.class);

    addStageFile(service, metadata.metadataLocation(), "metadata");
    MaterializeMetadataService.MaterializeResult result =
        service.materialize("sales.us", "trino_order70", metadata, metadata.metadataLocation());

    String canonical =
        readFile(service.fileIO(), MirrorLocationUtil.stripMetadataMirrorPrefix(result.metadataLocation()));
    assertTrue(canonical.contains("\"current-snapshot-id\":11"));
    assertTrue(canonical.contains("\"snapshot-id\":11"));
  }

  @Test
  void canonicalMaterializationRollsMetadataVersionWhenFileExists() throws IOException {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    String canonicalLocation =
        "s3://floecat/sales/us/trino_order71/metadata/00001-current.metadata.json";
    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://floecat/sales/us/trino_order71",
              "metadata-location": "s3://floecat/sales/us/trino_order71/metadata/00001-current.metadata.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "last-sequence-number": 1,
              "schemas": [],
              "partition-specs": [],
              "sort-orders": [],
              "refs": {},
              "snapshot-log": [],
              "metadata-log": [],
              "statistics": [],
              "partition-statistics": [],
              "snapshots": [
                {
                  "snapshot-id": 1,
                  "sequence-number": 1,
                  "manifest-list": "s3://floecat/sales/us/trino_order71/metadata/snap-1.avro"
                }
              ]
            }
            """,
            TableMetadataView.class);

    addStageFile(service, canonicalLocation, "existing");
    MaterializeMetadataService.MaterializeResult result =
        service.materialize("sales.us", "trino_order71", metadata, metadata.metadataLocation());

    assertTrue(result.metadataLocation().contains("00002"));
    assertTrue(service.fileIO().fileExists(result.metadataLocation()));
  }

  @Test
  void mirrorMetadataLocationRemainsIsolatedUntilPromoted() throws IOException {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    String mirrorLocation =
        "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata/00000-stage.metadata.json";
    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://floecat/sales/us/trino_order62",
              "metadata-location": "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata/00000-stage.metadata.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "metadata-location": "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata/00000-stage.metadata.json",
                "write.metadata.path": "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata",
                "s3.region": "us-east-1"
              },
              "last-column-id": 1,
              "current-schema-id": 1,
              "default-spec-id": 0,
              "last-partition-id": 0,
              "default-sort-order-id": 0,
              "last-sequence-number": 0,
              "schemas": [
                {
                  "type": "struct",
                  "schema-id": 1,
                  "last-column-id": 1,
                  "identifier-field-ids": [],
                  "fields": [
                    {
                      "id": 1,
                      "name": "id",
                      "required": true,
                      "type": "long"
                    }
                  ]
                }
              ],
              "partition-specs": [
                {
                  "spec-id": 0,
                  "fields": []
                }
              ],
              "sort-orders": [
                {
                  "order-id": 0,
                  "fields": []
                }
              ],
              "refs": {},
              "snapshot-log": [],
              "metadata-log": [],
              "statistics": [],
              "partition-statistics": [],
              "snapshots": [
                {
                  "snapshot-id": 1,
                  "sequence-number": 1,
                  "manifest-list": "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata/snap-1.avro"
                }
              ]
            }
            """,
            TableMetadataView.class);

    addStageFile(service, mirrorLocation, "stage");
    addStageFile(
        service,
        "s3://floecat/.floecat-metadata/sales/us/trino_order62/metadata/snap-1.avro",
        "snapshot");

    MaterializeMetadataService.MaterializeResult result =
        service.materialize("sales.us", "trino_order62", metadata, metadata.metadataLocation());

    assertEquals(mirrorLocation, result.metadataLocation());
    assertTrue(service.fileIO().fileExists(mirrorLocation));
    String canonical = MirrorLocationUtil.stripMetadataMirrorPrefix(mirrorLocation);
    assertFalse(service.fileIO().fileExists(canonical));
  }

  private void addStageFile(TestMaterializeMetadataService service, String location, String payload)
      throws IOException {
    try (PositionOutputStream stream =
        service.fileIO().newOutputFile(location).createOrOverwrite()) {
      stream.write(payload.getBytes(StandardCharsets.UTF_8));
    }
  }

  private String readFile(FileIO io, String location) throws IOException {
    InputFile input = io.newInputFile(location);
    try (SeekableInputStream stream = input.newStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static final class TestMaterializeMetadataService extends MaterializeMetadataService {
    private final PrefixAwareInMemoryFileIO fileIO = new PrefixAwareInMemoryFileIO();

    @Override
    protected FileIO newFileIo(Map<String, String> props) {
      return fileIO;
    }

    PrefixAwareInMemoryFileIO fileIO() {
      return fileIO;
    }
  }

  private static final class PrefixAwareInMemoryFileIO
      implements SupportsPrefixOperations {
    private final ConcurrentMap<String, byte[]> files = new ConcurrentHashMap<>();

    @Override
    public InputFile newInputFile(String location) {
      return new InMemoryInputFile(location);
    }

    @Override
    public InputFile newInputFile(String location, long length) {
      return newInputFile(location);
    }

    @Override
    public OutputFile newOutputFile(String location) {
      return new InMemoryOutputFile(location);
    }

    @Override
    public void deleteFile(String location) {
      files.remove(location);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      List<FileInfo> infos = new ArrayList<>();
      files.forEach(
          (path, data) -> {
            if (path.startsWith(prefix)) {
              infos.add(new FileInfo(path, data.length, System.currentTimeMillis()));
            }
          });
      return infos;
    }

    @Override
    public void deletePrefix(String prefix) {
      files.keySet().removeIf(path -> path.startsWith(prefix));
    }

    private final class InMemoryInputFile implements InputFile {
      private final String location;

      InMemoryInputFile(String location) {
        this.location = location;
      }

      @Override
      public long getLength() {
        byte[] data = files.get(location);
        if (data == null) {
          throw new UncheckedIOException(
              new java.io.FileNotFoundException("missing file " + location));
        }
        return data.length;
      }

      @Override
      public SeekableInputStream newStream() {
        byte[] data = files.get(location);
        if (data == null) {
          throw new UncheckedIOException(
              new java.io.FileNotFoundException("missing file " + location));
        }
        return new InMemorySeekableInputStream(data);
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public boolean exists() {
        return files.containsKey(location);
      }
    }

    private final class InMemoryOutputFile implements OutputFile {
      private final String location;

      InMemoryOutputFile(String location) {
        this.location = location;
      }

      @Override
      public PositionOutputStream create() {
        return createOrOverwrite();
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return new InMemoryPositionOutputStream(
            bytes -> files.put(location, bytes));
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public InputFile toInputFile() {
        return newInputFile(location);
      }
    }

    private static final class InMemoryPositionOutputStream extends PositionOutputStream {
      private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      private final Consumer<byte[]> onClose;

      InMemoryPositionOutputStream(Consumer<byte[]> onClose) {
        this.onClose = onClose;
      }

      @Override
      public long getPos() {
        return buffer.size();
      }

      @Override
      public void write(int b) {
        buffer.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) {
        buffer.write(b, off, len);
      }

      @Override
      public void close() {
        onClose.accept(buffer.toByteArray());
      }
    }

    private static final class InMemorySeekableInputStream extends SeekableInputStream {
      private final byte[] data;
      private int position = 0;

      InMemorySeekableInputStream(byte[] data) {
        this.data = data;
      }

      @Override
      public void seek(long newPos) {
        position = (int) newPos;
      }

      @Override
      public long getPos() {
        return position;
      }

      @Override
      public int read() {
        if (position >= data.length) {
          return -1;
        }
        return data[position++] & 0xFF;
      }

      @Override
      public int read(byte[] b, int off, int len) {
        if (position >= data.length) {
          return -1;
        }
        int remaining = data.length - position;
        int toRead = Math.min(len, remaining);
        System.arraycopy(data, position, b, off, toRead);
        position += toRead;
        return toRead;
      }

      @Override
      public void close() {}
    }
  }
}
